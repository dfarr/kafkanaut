package eventbus

import "context"
import "os"
import "strings"

import . "github.com/dfarr/kafkanaut/sensor"
import "github.com/dfarr/kafkanaut/eventbus/common"
import "github.com/dfarr/kafkanaut/eventbus/kafka"
import "github.com/dfarr/kafkanaut/eventbus/pulsar"


func GetSensorDriver(ctx context.Context, sensor Sensor) common.SensorDriver {
	switch strings.ToLower(os.Getenv("EB")) {
	case "kafka":
		return &kafka.SensorDriver{
			Sensor: sensor,
			Brokers: []string{"localhost:9092"},
		}
	default:
		return &pulsar.SensorDriver{
			Sensor: sensor,
			Broker: "pulsar://localhost:6650",
		}
	}
}
