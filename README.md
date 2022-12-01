# kafkanaut
Argo Events Sensor POC

## Prerequisites
- kafka broker
- pulsar broker

## Run
**kafka**
```
SLACK=https://hooks.slack.com/services/xxx EB=kafka go run ./...
```

**pulsar**
```
SLACK=https://hooks.slack.com/services/xxx EB=pulsar go run ./...
```
