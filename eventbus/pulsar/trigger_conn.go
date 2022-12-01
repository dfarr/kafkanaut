package pulsar

import "context"
import "encoding/json"
import "fmt"

import "github.com/apache/pulsar-client-go/pulsar"
import cloudevents "github.com/cloudevents/sdk-go/v2"

import . "github.com/dfarr/kafkanaut/sensor"


type TriggerConnection struct {
	driver          *SensorDriver
	sensorName      string
	triggerName     string
	depExpression   string
	dependencies    []Dependency
	msgIDs          []pulsar.MessageID
	events          []*EventPair
}

type EventPair struct {
	msgID           pulsar.MessageID
	event           *cloudevents.Event
}

func (c *TriggerConnection) Subscribe(ctx context.Context, actionFn func(string, string, []*cloudevents.Event) error) error {
	barCh := c.driver.Register("bar", c.triggerName)
	bazCh := c.driver.Register("baz", c.triggerName)

	defer close(barCh)
	defer close(bazCh)

	for {
		select {
		case req := <-barCh:
			fmt.Println("Update:", c.triggerName)

			if err := c.Update(req.msg); err != nil {
				req.ch <- Response{nil, nil, nil, err}
			} else {
				req.ch <- Response{c.Msgs(), c.Acks(), c.driver.bazProducer, nil}
			}
		case req := <-bazCh:
			fmt.Println("Triggered:", c.triggerName)

			req.ch <- Response{nil, []pulsar.MessageID{req.msg.ID()}, nil, nil}

			var action *cloudevents.Event
			var events []*cloudevents.Event

			if err := json.Unmarshal(req.msg.Payload(), &action); err != nil {
				fmt.Println(err)
				continue
			}

			if err := json.Unmarshal(action.Data(), &events); err != nil {
				fmt.Println(err)
				continue
			}

			if err := actionFn("Pulsar", c.triggerName, events); err != nil {
				fmt.Println(err)
				continue
			}
		}
	}

	return nil
}

func (c *TriggerConnection) Close() error {
	return c.driver.Close()
}

func (c *TriggerConnection) IsClosed() bool {
	return !c.driver.open
}

func (c *TriggerConnection) Update(msg pulsar.Message) error {
	var event *cloudevents.Event
	if err := json.Unmarshal(msg.Payload(), &event); err != nil {
		return err
	}

	c.msgIDs = append(c.msgIDs, msg.ID())
	eventPair := &EventPair{msg.ID(), event}

	found := false
	for i:=0; i<len(c.events); i++ {
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

func (c *TriggerConnection) Msgs() []*pulsar.ProducerMessage {
	var msgs []*pulsar.ProducerMessage

	if len(c.events) == len(c.dependencies) {
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

		// TODO: handle error
		payload, _ := json.Marshal(event)
		msgs = append(msgs, &pulsar.ProducerMessage{
			Key: c.triggerName,
			Payload: payload,
		})

		// reset
		c.events = []*EventPair{}
	}

	return msgs
}

func (c *TriggerConnection) Acks() []pulsar.MessageID {
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
