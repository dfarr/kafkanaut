package pulsar

import "context"
import "encoding/json"
import "errors"
import "fmt"
import "regexp"
import "sync"
import "time"

import "github.com/apache/pulsar-client-go/pulsar"
import cloudevents "github.com/cloudevents/sdk-go/v2"

import . "github.com/dfarr/kafkanaut/sensor"
import "github.com/dfarr/kafkanaut/eventbus/common"


type SensorDriver struct {
	sync.Mutex
	Sensor      Sensor
	Broker      string
	handlers    map[string]chan Request
	client      pulsar.Client
	consumer    pulsar.Consumer
	barProducer pulsar.Producer
	bazProducer pulsar.Producer
	open        bool
}

type Request struct {
	msg           pulsar.Message
	ch            chan Response
}

type Response struct {
	msgs          []*pulsar.ProducerMessage
	acks          []pulsar.MessageID
	producer      pulsar.Producer
	err           error
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
		Topics: []string{"foo", "bar", "baz"},
		SubscriptionName: d.Sensor.Name,
		Type: pulsar.Failover,
	})

	if err != nil {
		return err
	}

	// producers
	barProducer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "bar",
		BatcherBuilderType: pulsar.KeyBasedBatchBuilder,
	})

	if err != nil {
		return err
	}

	bazProducer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "baz",
		BatcherBuilderType: pulsar.KeyBasedBatchBuilder,
	})

	if err != nil {
		return err
	}

	d.client = client
	d.consumer = consumer
	d.barProducer = barProducer
	d.bazProducer = bazProducer

	// TODO: should this be done in initialize?
	go d.FanOut()

	return nil
}

func (d *SensorDriver) Close() error {
	d.consumer.Close()
	d.barProducer.Close()
	d.bazProducer.Close()
	d.client.Close()

	for _, handler := range d.handlers {
		close(handler)
	}

	return nil
}

func (d *SensorDriver) Connect(ctx context.Context, triggerName string, depExpression string, dependencies []Dependency) (common.TriggerConnection, error) {
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

		key := fmt.Sprintf("%s/%s", topic, msg.Key())

		if handler, ok := d.handlers[key]; ok {
			ch := make(chan Response)

			handler <- Request{msg, ch}

			// do this asynchronously
			go func(ch chan Response) {
				res := <-ch
				defer close(ch)

				if res.err != nil {
					fmt.Println(res.err)
					return
				}

				// pretend this is in a transaction

				for _, msg := range res.msgs {
					res.producer.Send(ctx, msg)
				}

				for _, ack := range res.acks {
					d.consumer.AckID(ack)
				}
			}(ch)

		} else {
			d.consumer.Ack(msg)
		}
	}
}

func (d *SensorDriver) FanOut() {
	// Register a single handler for all messages on topic 'foo'.
	// This could be optimized by creating handlers on seperate
	// go routines for each sensor dependency.
	ch := d.Register("foo", "")
	defer close(ch)

	for {
		req := <-ch

		var event *cloudevents.Event
		if err := json.Unmarshal(req.msg.Payload(), &event); err != nil {
			req.ch <- Response{nil, nil, nil, err}
			continue
		}

		var msgs []*pulsar.ProducerMessage
		for _, trigger := range d.Sensor.Triggers {
			for _, dependency := range trigger.Dependencies {
				if dependency.EventSourceName == event.Source() && dependency.EventName == event.Subject() {
					msgs = append(msgs, &pulsar.ProducerMessage{
						Key: trigger.Name,
						Payload: req.msg.Payload(),
					})
				}
			}
		}

		req.ch <- Response{msgs, []pulsar.MessageID{req.msg.ID()}, d.barProducer, nil}
	}
}

func (d *SensorDriver) Register(topic string, key string) chan Request {
	d.Lock()
	defer d.Unlock()

	ch := make(chan Request, 100)

	if d.handlers == nil {
		d.handlers = make(map[string]chan Request)
	}

	d.handlers[fmt.Sprintf("%s/%s", topic, key)] = ch

	return ch
}

func (d *SensorDriver) isReady() bool {
	// TODO: fix
	// *2 for bar/baz topic handlers (per trigger)
	// +1 for the foo topic handler
	total := len(d.Sensor.Triggers)*2 + 1

	return len(d.handlers) == total
}

func extractTopic(topic string) (string, error) {
	re := regexp.MustCompile(`^persistent:\/\/public\/default\/(.*)-partition-\d*$`)

	if !re.MatchString(topic) {
		return "", errors.New("Invalid topic")
	}

	return re.FindStringSubmatch(topic)[1], nil
}
