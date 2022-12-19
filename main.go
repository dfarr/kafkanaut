package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/dfarr/kafkanaut/eventbus"
	. "github.com/dfarr/kafkanaut/sensor"
)

func main() {
	// fake sensor
	dep1 := Dependency{Name: "d1", EventSourceName: "es-1", EventName: "blue"}
	dep2 := Dependency{Name: "d2", EventSourceName: "es-2", EventName: "yellow"}
	dep3 := Dependency{Name: "d3", EventSourceName: "es-3", EventName: "red"}

	sensor := Sensor{
		Name: "sensor-1",
		Triggers: []Trigger{
			{Name: "trigger-1", Dependencies: []Dependency{dep1}},
			{Name: "trigger-2", Dependencies: []Dependency{dep1, dep2}},
			{Name: "trigger-3", Dependencies: []Dependency{dep1, dep2, dep3}},
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
		"text": fmt.Sprintf("*%s*\n%s: ```%s```", kind, trigger, strings.Join(data, "\n")),
	})

	res, err := http.Post(os.Getenv("SLACK"), "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	res.Body.Close()

	return nil
}
