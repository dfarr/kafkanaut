package kafka

import "context"
import "encoding/json"
import "fmt"

import "github.com/Shopify/sarama"
import cloudevents "github.com/cloudevents/sdk-go/v2"

import . "github.com/dfarr/kafkanaut/sensor"


type TriggerConnection struct {
	driver          *SensorDriver
	sensorName      string
	triggerName     string
	depExpression   string
	dependencies    []Dependency
	events          []EventData
	action          func(string, string, []*cloudevents.Event) error
}

type EventData struct {
	partition       int32
	offset          int64
	event           *cloudevents.Event
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

func (c *TriggerConnection) Handle(msg *sarama.ConsumerMessage) (*cloudevents.Event, error) {
	var event *cloudevents.Event

	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return nil, err
	}

	eventData := EventData{
		msg.Partition,
		msg.Offset,
		event,
	}

	found := false

	for i:=0; i<len(c.events); i++ {
		if c.events[i].event.Source() == event.Source() && c.events[i].event.Subject() == event.Subject() {
			c.events[i] = eventData
			found = true
			break
		}
	}

	if !found {
		c.events = append(c.events, eventData)
	}

	// trigger action
	if len(c.events) == len(c.dependencies) {
		events := []*cloudevents.Event{}
		for _, eventData := range c.events {
			events = append(events, eventData.event)
		}

		action := cloudevents.NewEvent()
		action.SetID(event.ID())
		action.SetSource(c.sensorName)
		action.SetSubject(c.triggerName)
		action.SetData(cloudevents.ApplicationJSON, events)

		c.events = []EventData{}

		return &action, nil
	}

	return nil, nil
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

func (c *TriggerConnection) HighWatermark(partition int32, offset int64) int64 {
	for _, event := range c.events {
		if partition == event.partition && offset > event.offset {
			offset = event.offset
		}
	}

	return offset
}
