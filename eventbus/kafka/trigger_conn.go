package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	. "github.com/dfarr/kafkanaut/sensor"
)

type TriggerConnection struct {
	driver        *SensorDriver
	sensorName    string
	triggerName   string
	depExpression string
	dependencies  []Dependency
	events        []EventData
	action        func(string, string, []*cloudevents.Event) error
}

type EventData struct {
	partition int32
	offset    int64
	event     *cloudevents.Event
}

func (c *TriggerConnection) Close() error {
	return c.driver.Close()
}

func (c *TriggerConnection) IsClosed() bool {
	return !c.driver.open
}

func (c *TriggerConnection) Subscribe(ctx context.Context, action func(string, string, []*cloudevents.Event) error) error {
	c.action = action
	c.driver.Register(c)

	return nil
}

func (c *TriggerConnection) Name() string {
	return c.triggerName
}

func (c *TriggerConnection) Update(msg *sarama.ConsumerMessage) error {
	var event *cloudevents.Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return err
	}

	found := false
	eventData := EventData{
		msg.Partition,
		msg.Offset,
		event,
	}

	for i := 0; i < len(c.events); i++ {
		if c.events[i].event.Source() == event.Source() && c.events[i].event.Subject() == event.Subject() {
			c.events[i] = eventData
			found = true
			break
		}
	}

	if !found {
		c.events = append(c.events, eventData)
	}

	return nil
}

func (c *TriggerConnection) Satisfied() bool {
	return len(c.events) == len(c.dependencies)
}

func (c *TriggerConnection) Offset(partition int32, offset int64) int64 {
	for _, event := range c.events {
		if partition == event.partition && offset > event.offset {
			offset = event.offset
		}
	}

	return offset
}

func (c *TriggerConnection) Action() (*sarama.ProducerMessage, error) {
	if !c.Satisfied() {
		return nil, nil
	}

	id := ""
	events := []*cloudevents.Event{}
	for _, eventData := range c.events {
		events = append(events, eventData.event)
		id = eventData.event.ID()
	}

	action := cloudevents.NewEvent()
	action.SetID(id)
	action.SetSource(c.sensorName)
	action.SetSubject(c.triggerName)
	action.SetData(cloudevents.ApplicationJSON, events)

	value, err := json.Marshal(action)
	if err != nil {
		return nil, err
	}

	return &sarama.ProducerMessage{
		Topic: "action",
		Key:   sarama.StringEncoder(c.Name()),
		Value: sarama.ByteEncoder(value),
	}, nil
}

func (c *TriggerConnection) Reset() {
	c.events = []EventData{}
}

func (c *TriggerConnection) Execute(msg *sarama.ConsumerMessage) error {
	var action *cloudevents.Event
	var events []*cloudevents.Event

	if err := json.Unmarshal(msg.Value, &action); err != nil {
		return err
	}

	if err := json.Unmarshal(action.Data(), &events); err != nil {
		return err
	}

	fmt.Printf("Triggered: %s\n", c.triggerName)
	return c.action("Kafka", c.triggerName, events)
}
