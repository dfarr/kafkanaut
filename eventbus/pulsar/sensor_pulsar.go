package pulsar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/dfarr/kafkanaut/eventbus/common"
	. "github.com/dfarr/kafkanaut/sensor"
)

type SensorDriver struct {
	sync.Mutex
	Sensor          Sensor
	Broker          string
	client          pulsar.Client
	consumer        pulsar.Consumer
	triggerProducer pulsar.Producer
	actionProducer  pulsar.Producer
	handlers        map[string]*TriggerConnection
	open            bool
}

func (d *SensorDriver) Initialize() error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: d.Broker,
	})

	if err != nil {
		return err
	}

	// consumer
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topics:           []string{"event", "trigger", "action"},
		SubscriptionName: d.Sensor.Name,
		Type:             pulsar.Failover,
	})

	if err != nil {
		return err
	}

	// producers
	triggerProducer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:              "trigger",
		BatcherBuilderType: pulsar.KeyBasedBatchBuilder,
	})

	if err != nil {
		return err
	}

	actionProducer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:              "action",
		BatcherBuilderType: pulsar.KeyBasedBatchBuilder,
	})

	if err != nil {
		return err
	}

	d.client = client
	d.consumer = consumer
	d.triggerProducer = triggerProducer
	d.actionProducer = actionProducer

	return nil
}

func (d *SensorDriver) Close() error {
	d.consumer.Close()
	d.triggerProducer.Close()
	d.actionProducer.Close()
	d.client.Close()

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

		msg, err := d.consumer.Receive(ctx)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Printf("Received: topic=%s key=%s payload=%s\n", msg.Topic(), msg.Key(), string(msg.Payload()))

		topic, err := extractTopic(msg.Topic())
		if err != nil {
			fmt.Println(err)
			return
		}

		if topic == "event" {
			if err := d.Event(ctx, msg); err != nil {
				fmt.Println(err)
			}
		}

		if topic == "trigger" {
			if err := d.Trigger(ctx, msg); err != nil {
				fmt.Println(err)
			}
		}

		if topic == "action" {
			if err := d.Action(ctx, msg); err != nil {
				fmt.Println(err)
			}
		}
	}
}

func (d *SensorDriver) Register(handler *TriggerConnection) {
	d.Lock()
	defer d.Unlock()

	if d.handlers == nil {
		d.handlers = make(map[string]*TriggerConnection)
	}

	d.handlers[handler.Name()] = handler
}

func (d *SensorDriver) isReady() bool {
	return len(d.handlers) == len(d.Sensor.Triggers)
}

func (d *SensorDriver) Event(ctx context.Context, msg pulsar.Message) error {
	var event *cloudevents.Event
	if err := json.Unmarshal(msg.Payload(), &event); err != nil {
		return err
	}

	// pretend this happens in a transaction

	for _, trigger := range d.Sensor.Triggers {
		for _, dependency := range trigger.Dependencies {
			if dependency.EventSourceName == event.Source() && dependency.EventName == event.Subject() {
				d.triggerProducer.Send(ctx, &pulsar.ProducerMessage{
					Key:     trigger.Name,
					Payload: msg.Payload(),
				})
			}
		}
	}

	return d.consumer.Ack(msg)
}

func (d *SensorDriver) Trigger(ctx context.Context, msg pulsar.Message) error {
	if handler, ok := d.handlers[msg.Key()]; ok {
		if err := handler.Update(msg); err != nil {
			return err
		}

		// pretend this happens in a transaction

		if handler.Satisfied() {
			actionMsg, err := handler.Action()
			if err != nil {
				return err
			}

			d.actionProducer.Send(ctx, actionMsg)
			handler.Reset()
		}

		for _, id := range handler.Ack() {
			d.consumer.AckID(id)
		}
	} else {
		d.consumer.Ack(msg)
	}

	return nil
}

func (d *SensorDriver) Action(ctx context.Context, msg pulsar.Message) error {
	if handler, ok := d.handlers[msg.Key()]; ok {
		if err := handler.Execute(msg); err != nil {
			return err
		}
	}

	return d.consumer.Ack(msg)
}

func extractTopic(topic string) (string, error) {
	re := regexp.MustCompile(`^persistent:\/\/public\/default\/(.*)-partition-\d*$`)

	if !re.MatchString(topic) {
		return "", errors.New("invalid topic")
	}

	return re.FindStringSubmatch(topic)[1], nil
}
