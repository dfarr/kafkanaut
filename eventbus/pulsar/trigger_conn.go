package pulsar

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	. "github.com/dfarr/kafkanaut/sensor"
)

type TriggerConnection struct {
	driver        *SensorDriver
	sensorName    string
	triggerName   string
	depExpression string
	dependencies  []Dependency
	msgIDs        []pulsar.MessageID
	events        []*EventPair
	action        func(string, string, []*cloudevents.Event) error
}

type EventPair struct {
	msgID pulsar.MessageID
	event *cloudevents.Event
}

func (c *TriggerConnection) Subscribe(ctx context.Context, action func(string, string, []*cloudevents.Event) error) error {
	c.action = action
	c.driver.Register(c)

	return nil
}

func (c *TriggerConnection) Close() error {
	return c.driver.Close()
}

func (c *TriggerConnection) IsClosed() bool {
	return !c.driver.open
}

func (c *TriggerConnection) Name() string {
	return c.triggerName
}

func (c *TriggerConnection) Update(msg pulsar.Message) error {
	var event *cloudevents.Event
	if err := json.Unmarshal(msg.Payload(), &event); err != nil {
		return err
	}

	c.msgIDs = append(c.msgIDs, msg.ID())
	eventPair := &EventPair{msg.ID(), event}

	found := false
	for i := 0; i < len(c.events); i++ {
		if c.events[i].event.Source() == event.Source() && c.events[i].event.Subject() == event.Subject() {
			c.events[i] = eventPair
			found = true
			break
		}
	}

	if !found {
		c.events = append(c.events, eventPair)
	}

	return nil
}

func (c *TriggerConnection) Satisfied() bool {
	return len(c.events) == len(c.dependencies)
}

func (c *TriggerConnection) MessageIDs() []pulsar.MessageID {
	msgIDs := []pulsar.MessageID{}
	i := 0

	for _, msgID := range c.msgIDs {
		found := false
		for _, eventPair := range c.events {
			if msgID == eventPair.msgID {
				found = true
				break
			}
		}

		if !found {
			msgIDs = append(msgIDs, msgID)
		} else {
			c.msgIDs[i] = msgID
			i++
		}
	}

	return msgIDs
}

func (c *TriggerConnection) Action() (*pulsar.ProducerMessage, error) {
	if !c.Satisfied() {
		return nil, nil
	}

	var id string
	var events []*cloudevents.Event
	for _, eventPair := range c.events {
		id = eventPair.event.ID()
		events = append(events, eventPair.event)
	}

	event := cloudevents.NewEvent()
	event.SetID(id)
	event.SetSource(c.sensorName)
	event.SetSubject(c.triggerName)
	event.SetData(cloudevents.ApplicationJSON, events)

	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	return &pulsar.ProducerMessage{
		Key:     c.Name(),
		Payload: payload,
	}, nil
}

func (c *TriggerConnection) Reset() {
	c.events = []*EventPair{}
}

func (c *TriggerConnection) Execute(msg pulsar.Message) error {
	var action *cloudevents.Event
	var events []*cloudevents.Event

	if err := json.Unmarshal(msg.Payload(), &action); err != nil {
		return err
	}

	if err := json.Unmarshal(action.Data(), &events); err != nil {
		return err
	}

	fmt.Printf("Triggered: %s\n", c.triggerName)
	return c.action("Pulsar", c.triggerName, events)
}
