package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/dfarr/kafkanaut/eventbus/common"
	. "github.com/dfarr/kafkanaut/sensor"
)

type SensorDriver struct {
	sync.Mutex
	Sensor        Sensor
	Brokers       []string
	client        sarama.Client
	offsetManager sarama.OffsetManager
	consumer      sarama.ConsumerGroup
	producer      sarama.AsyncProducer
	handlers      []*TriggerConnection
	metadata      map[string]string
	open          bool
	setup         bool
}

func (d *SensorDriver) Initialize() error {
	config := sarama.NewConfig()

	// consumer config
	config.Consumer.IsolationLevel = sarama.ReadCommitted
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// producer config for exactly once
	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Transaction.ID = fmt.Sprintf("%s-%d", d.Sensor.Name, os.Getpid())
	config.Net.MaxOpenRequests = 1

	client, err := sarama.NewClient(d.Brokers, config)
	if err != nil {
		return err
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(d.Sensor.Name, client)
	if err != nil {
		return err
	}

	consumer, err := sarama.NewConsumerGroupFromClient(d.Sensor.Name, client)
	if err != nil {
		return err
	}

	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return err
	}

	d.client = client
	d.offsetManager = offsetManager
	d.consumer = consumer
	d.producer = producer

	return nil
}

func (d *SensorDriver) Close() error {
	if err := d.consumer.Close(); err != nil {
		return err
	}

	if err := d.producer.Close(); err != nil {
		return err
	}

	return nil
}

func (d *SensorDriver) Connect(ctx context.Context, triggerName string, depExpression string, dependencies []Dependency) (common.TriggerConnection, error) {
	d.Lock()
	defer d.Unlock()

	if !d.open {
		d.open = true
		go d.listen(ctx)
	}

	return &TriggerConnection{
		driver:        d,
		sensorName:    d.Sensor.Name,
		triggerName:   triggerName,
		depExpression: depExpression,
		dependencies:  dependencies,
	}, nil
}

func (d *SensorDriver) listen(ctx context.Context) {
	defer func() {
		d.open = false
	}()

	for {
		if !d.isReady() {
			// wait until ready
			time.Sleep(10 * time.Second)
			continue
		}

		if err := d.consumer.Consume(ctx, []string{"event", "trigger", "action"}, d); err != nil {
			fmt.Println(err)
			return
		}

		if err := ctx.Err(); err != nil {
			fmt.Println(err)
			return
		}
	}
}

func (d *SensorDriver) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			fmt.Printf("Received: topic=%s partition=%d offset=%d\n", msg.Topic, msg.Partition, msg.Offset)

			if msg.Topic == "event" {
				if err := d.Event(msg, session); err != nil {
					fmt.Println(err)
				}
			}

			if msg.Topic == "trigger" {
				if err := d.Trigger(msg, session); err != nil {
					fmt.Println(err)
				}
			}

			if msg.Topic == "action" {
				if err := d.Action(msg, session); err != nil {
					fmt.Println(err)
				}
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func (d *SensorDriver) Setup(session sarama.ConsumerGroupSession) error {
	if d.metadata == nil {
		d.metadata = map[string]string{}
	}

	for _, partition := range session.Claims()["action"] {
		partitionOffsetManager, err := d.offsetManager.ManagePartition("action", partition)
		if err != nil {
			return err
		}

		_, md := partitionOffsetManager.NextOffset()

		var metadata map[string]string
		if err := json.Unmarshal([]byte(md), &metadata); err != nil {
			continue
		}

		for trigger, id := range metadata {
			d.metadata[trigger] = id
		}
	}

	d.setup = true

	return nil
}

func (d *SensorDriver) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (d *SensorDriver) Register(handler *TriggerConnection) {
	d.Lock()
	defer d.Unlock()

	found := false

	for i := 0; i < len(d.handlers); i++ {
		if d.handlers[i].Name() == handler.Name() {
			d.handlers[i] = handler
			found = true
			break
		}
	}

	if !found {
		d.handlers = append(d.handlers, handler)
	}
}

func (d *SensorDriver) isReady() bool {
	return d.setup && len(d.handlers) == len(d.Sensor.Triggers)
}

func (d *SensorDriver) Event(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
	var event *cloudevents.Event

	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return err
	}

	d.Lock()
	defer d.Unlock()

	// TODO: consolidate kafka transaction in a function
	if err := d.producer.BeginTxn(); err != nil {
		return err
	}

	if err := d.producer.AddMessageToTxn(msg, d.Sensor.Name, nil); err != nil {
		fmt.Println(err)
		d.handleTxnError(msg, session, err, func() error {
			return d.producer.AddMessageToTxn(msg, d.Sensor.Name, nil)
		})
		return nil
	}

	for _, trigger := range d.Sensor.Triggers {
		for _, dependency := range trigger.Dependencies {
			if dependency.EventSourceName == event.Source() && dependency.EventName == event.Subject() {
				d.producer.Input() <- &sarama.ProducerMessage{
					Topic: "trigger",
					Key:   sarama.StringEncoder(trigger.Name),
					Value: sarama.ByteEncoder(msg.Value),
				}
			}
		}
	}

	if err := d.producer.CommitTxn(); err != nil {
		fmt.Println(err)
		d.handleTxnError(msg, session, err, func() error {
			return d.producer.CommitTxn()
		})
	}

	return nil
}

func (d *SensorDriver) Trigger(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
	d.Lock()
	defer d.Unlock()

	if err := d.producer.BeginTxn(); err != nil {
		return err
	}

	offset := msg.Offset + 1

	for _, handler := range d.handlers {
		if handler.Name() == string(msg.Key) {
			if err := handler.Update(msg); err != nil {
				return err
			}
		}

		if handler.Satisfied() {
			actionMsg, err := handler.Action()
			if err != nil {
				return err
			}

			// add header
			actionMsg.Headers = []sarama.RecordHeader{{Key: []byte(""), Value: []byte(strconv.Itoa(int(msg.Offset)))}}

			d.producer.Input() <- actionMsg
			handler.Reset()
		}

		offset = handler.Offset(msg.Partition, offset)
	}

	offsets := map[string][]*sarama.PartitionOffsetMetadata{
		msg.Topic: {{
			Partition: msg.Partition,
			Offset:    offset,
			Metadata:  nil,
		}},
	}

	if err := d.producer.AddOffsetsToTxn(offsets, d.Sensor.Name); err != nil {
		fmt.Println(err)
		d.handleTxnError(msg, session, err, func() error {
			return d.producer.AddMessageToTxn(msg, d.Sensor.Name, nil)
		})
		return nil
	}

	// If no messages are produced in the transaction, but offsets
	// are bumped (which can happen) the transaction has no effect
	// and the offsets remain set to what they were before, ignoring
	// this for now.
	if err := d.producer.CommitTxn(); err != nil {
		fmt.Println(err)
		d.handleTxnError(msg, session, err, func() error {
			return d.producer.CommitTxn()
		})
	}

	return nil
}

func (d *SensorDriver) Action(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
	var event *cloudevents.Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return err
	}

	id := d.metadata[string(msg.Key)]

	if id != event.ID() {
		for _, handler := range d.handlers {
			if handler.Name() == string(msg.Key) {
				if err := handler.Execute(msg); err != nil {
					return err
				}
			}
		}
	} else {
		fmt.Println("Already processed, skipping...")
	}

	d.metadata[string(msg.Key)] = event.ID()
	metadata, _ := json.Marshal(d.metadata)

	session.MarkMessage(msg, string(metadata))
	session.Commit()

	return nil
}

func (d *SensorDriver) handleTxnError(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession, err error, defaulthandler func() error) {
	fmt.Printf("Message consumer: unable to process transaction: %+v\n", err)

	for {
		if d.producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
			// fatal error. need to recreate producer.
			fmt.Println("Message consumer: producer is in a fatal state, need to recreate it")
			// reset current consumer offset to retry consume this record.
			session.ResetOffset(msg.Topic, msg.Partition, msg.Offset, "")
			return
		}
		if d.producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
			if err = d.producer.AbortTxn(); err != nil {
				fmt.Printf("Message consumer: unable to abort transaction: %+v\n", err)
				continue
			}
			// reset current consumer offset to retry consume this record.
			session.ResetOffset(msg.Topic, msg.Partition, msg.Offset, "")
			return
		}

		// if not you can retry
		if err = defaulthandler(); err == nil {
			return
		}
	}
}
