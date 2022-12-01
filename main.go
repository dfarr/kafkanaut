package main

import "bytes"
import "context"
import "encoding/json"
import "fmt"
import "os"
import "net/http"
import "sync"
import "strings"
import "time"

import cloudevents "github.com/cloudevents/sdk-go/v2"

import . "github.com/dfarr/kafkanaut/sensor"
import "github.com/dfarr/kafkanaut/eventbus"


func main() {
	// fake sensor
	dep1 := Dependency{"d1", "es-1", "blue"}
	dep2 := Dependency{"d2", "es-2", "yellow"}
	dep3 := Dependency{"d3", "es-3", "red"}

	sensor := Sensor{
		Name: "sensor-1",
		Triggers: []Trigger{
			Trigger{"trigger-1", []Dependency{dep1}},
			Trigger{"trigger-2", []Dependency{dep1, dep2}},
			Trigger{"trigger-3", []Dependency{dep1, dep2, dep3}},
		},
	}

	// context
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create driver
	driver := eventbus.GetSensorDriver(ctx, sensor)

	if err := driver.Initialize(); err != nil {
		panic(err)
	}

	// connect and subscribe for each trigger
	wg := &sync.WaitGroup{}
	for _, t := range sensor.Triggers {
		wg.Add(1)

		go func(trigger Trigger) {
			defer wg.Done()

			conn, err := driver.Connect(ctx, trigger.Name, "", trigger.Dependencies)
			if err != nil {
				panic(err)
			}

			defer conn.Close()

			go func() {
				if err := conn.Subscribe(ctx, action); err != nil {
					panic(err)
				}
			}()

			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if conn == nil || conn.IsClosed() {
						conn, err = driver.Connect(ctx, trigger.Name, "", trigger.Dependencies)
						if err != nil {
							fmt.Println(err)
						}
					}
				}
			}
		}(t)
	}

	wg.Wait()
}

// Action function
func action(kind string, trigger string, events []*cloudevents.Event) error {
	data := []string{}

	for _, event := range events {
		data = append(data, string(event.Data()))
	}

	body, _ := json.Marshal(map[string]string{
		"text":  fmt.Sprintf("*%s*\n%s: ```%s```", kind, trigger, strings.Join(data, "\n")),
	})

	res, err := http.Post(os.Getenv("SLACK"), "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	res.Body.Close()

	return nil
}
