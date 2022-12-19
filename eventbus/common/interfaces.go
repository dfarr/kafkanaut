package common

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	. "github.com/dfarr/kafkanaut/sensor"
)

type Connection interface {
	Close() error
	IsClosed() bool
}

type TriggerConnection interface {
	Connection
	Subscribe(ctx context.Context, action func(string, string, []*cloudevents.Event) error) error
}

type SensorDriver interface {
	Initialize() error
	Connect(ctx context.Context, triggerName string, depExpression string, dependencies []Dependency) (TriggerConnection, error)
}
