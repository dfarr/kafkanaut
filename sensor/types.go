package sensor

type Sensor struct {
	Name            string
	Triggers        []Trigger
}

type Trigger struct {
	Name            string
	Dependencies    []Dependency
}

type Dependency struct {
	Name            string
	EventSourceName string
	EventName       string
}
