package kafka

import "context"
import "encoding/json"
import "fmt"
import "os"
import "sync"
import "time"

import "github.com/Shopify/sarama"
import cloudevents "github.com/cloudevents/sdk-go/v2"

import . "github.com/dfarr/kafkanaut/sensor"
import "github.com/dfarr/kafkanaut/eventbus/common"


type SensorDriver struct {
	sync.Mutex
	Sensor          Sensor
	Brokers         []string
	triggers        []*TriggerConnection
	consumer        sarama.ConsumerGroup
	producer        sarama.AsyncProducer
	open            bool
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

	consumer, err := sarama.NewConsumerGroup(d.Brokers, d.Sensor.Name, config)
	if err != nil {
		return err
	}

	producer, err := sarama.NewAsyncProducer(d.Brokers, config)
	if err != nil {
		return err
	}

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
		driver: d,
		sensorName: d.Sensor.Name,
		triggerName: triggerName,
		depExpression: depExpression,
		dependencies: dependencies,
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

		if err := d.consumer.Consume(ctx, []string{"foo", "bar", "baz"}, d); err != nil {
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
		case msg := <- claim.Messages():
			fmt.Printf("Received: topic=%s partition=%d offset=%d\n", msg.Topic, msg.Partition, msg.Offset)

			if msg.Topic == "foo" {
				if err := d.Foo(msg, session); err != nil {
					fmt.Println(err)
				}
			}

			if msg.Topic == "bar" {
				if err := d.Bar(msg, session); err != nil {
					fmt.Println(err)
				}
			}

			if msg.Topic == "baz" {
				if err := d.Baz(msg, session); err != nil {
					fmt.Println(err)
				}
			}
		case <- session.Context().Done():
			return nil
		}
	}
}

func (d *SensorDriver) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (d *SensorDriver) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (d *SensorDriver) Register(trigger *TriggerConnection) {
	d.Lock()
	defer d.Unlock()

	found := false

	for i:=0; i<len(d.triggers); i++ {
		if d.triggers[i].triggerName == trigger.triggerName {
			d.triggers[i] = trigger
			found = true
			break
		}
	}

	if !found {
		d.triggers = append(d.triggers, trigger)
	}
}

func (d *SensorDriver) isReady() bool {
	return len(d.triggers) == len(d.Sensor.Triggers)
}

func (d *SensorDriver) Foo(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
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
					Topic: "bar",
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

func (d *SensorDriver) Bar(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
	offset := msg.Offset + 1
	actions := []*sarama.ProducerMessage{}

	for _, trigger := range d.triggers {
		if trigger.triggerName == string(msg.Key) {
			action, err := trigger.Handle(msg)
			if err != nil {
				return err
			}

			if action != nil {
				value, err := json.Marshal(action)
				if err != nil {
					return err
				}

				actions = append(actions, &sarama.ProducerMessage{
					Topic: "baz",
					Key:   sarama.StringEncoder(trigger.triggerName),
					Value: sarama.ByteEncoder(value),
				})
			}
		}

		offset = trigger.HighWatermark(msg.Partition, offset)
	}

	if len(actions) > 0 {
		d.Lock()
		defer d.Unlock()

		if err := d.producer.BeginTxn(); err != nil {
			return err
		}

		for _, action := range actions {
			d.producer.Input() <- action
		}

		metadata := ""
		offsets := map[string][]*sarama.PartitionOffsetMetadata{
			msg.Topic: {{
				msg.Partition,
				offset,
				&metadata,
			}},
		}
		if err := d.producer.AddOffsetsToTxn(offsets, d.Sensor.Name); err != nil {
			fmt.Println(err)
			d.handleTxnError(msg, session, err, func() error {
				return d.producer.AddMessageToTxn(msg, d.Sensor.Name, nil)
			})
			return nil
		}

		if err := d.producer.CommitTxn(); err != nil {
			fmt.Println(err)
			d.handleTxnError(msg, session, err, func() error {
				return d.producer.CommitTxn()
			})
		}
	} else {
		session.MarkOffset(msg.Topic, msg.Partition, offset, "")
		session.Commit()
	}

	return nil
}

func (d *SensorDriver) Baz(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
	for _, trigger := range d.triggers {
		if trigger.triggerName == string(msg.Key) {
			if err := trigger.Execute(msg); err != nil {
				return err
			}
		}
	}

	session.MarkMessage(msg, "")
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
