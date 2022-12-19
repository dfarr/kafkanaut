package eventbus

import (
	"context"
	"os"
	"strings"

	"github.com/dfarr/kafkanaut/eventbus/common"
	"github.com/dfarr/kafkanaut/eventbus/kafka"
	"github.com/dfarr/kafkanaut/eventbus/pulsar"
	. "github.com/dfarr/kafkanaut/sensor"
)

func GetSensorDriver(ctx context.Context, sensor Sensor) common.SensorDriver {
	switch strings.ToLower(os.Getenv("EB")) {
	case "kafka":
		return &kafka.SensorDriver{
			Sensor:  sensor,
			Brokers: []string{"localhost:9092"},
		}
	case "pulsar":
		return &pulsar.SensorDriver{
			Sensor: sensor,
			Broker: "pulsar://localhost:6650",
		}
	default:
		panic("You must specify EB={kafka, pulsar}")
	}
}
