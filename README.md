# kafkanaut
An exploration of messaging systems with [Argo Events](https://argoproj.github.io/argo-events/).

Implements two alternative EventBus technologies: [Kafka](https://kafka.apache.org/) and [Pulsar](https://pulsar.apache.org/). Both implementations enable horizontal scaling of Argo Event's Sensor applications, something not currently possible with vanilla Argo Events.

## Architecture
![architecture](https://user-images.githubusercontent.com/1387834/208555850-fcf1fd54-656d-4d91-a5c8-2d0497f9aa47.png)

## Setup
### Slack
A slack incoming webhook is required to run both the Kafka and Pulsar implementations. Follow [these instructions](https://slack.com/help/articles/115005265063-Incoming-webhooks-for-Slack) to set up a custom slack application with a webhook. Set the webhook URL as the `SLACK` environment variable when running the go program as described below.

### Kafka
To run the Kafka implementation you will need to have a [local broker](https://kafka.apache.org/quickstart) running on port 9092. The `{event, trigger, action}` topics will be automatically created if they do not exist. To play around with a different number of partitions you can run the following commands:
```
kafka-topics --bootstrap-server localhost:9092 --create --partitions 3 --topic event
kafka-topics --bootstrap-server localhost:9092 --create --partitions 3 --topic trigger
kafka-topics --bootstrap-server localhost:9092 --create --partitions 3 --topic action
```

### Pulsar
To run the Pulsar implementation you will need to have a [local broker](https://pulsar.apache.org/docs/2.10.x/getting-started-standalone/) running on port 6650. The `{event, trigger, action}` topics will be automatically created if they do not exist, but as non-partitioned topics. To create partitioned topics (as intended) you can run the following comands:
```
bin/pulsar-admin topics create-partitioned-topic -p 3 event
bin/pulsar-admin topics create-partitioned-topic -p 3 trigger
bin/pulsar-admin topics create-partitioned-topic -p 3 action
```

### Sample Messages
```json
{"specversion": "1.0", "id": "1", "source": "es-1", "subject": "blue", "data": "blue"}
{"specversion": "1.0", "id": "2", "source": "es-2", "subject": "yellow", "data": "yellow"}
{"specversion": "1.0", "id": "3", "source": "es-3", "subject": "red", "data": "red"}
```

## Run
### Kafka
```
EB=kafka SLACK=https://hooks.slack.com/services/xxx go run ./...
```

Multiple instances can be run simultaneously. Run the following command to produce test mesages:
```
kafka-console-producer --bootstrap-server localhost:9092 --topic event
> {"specversion": "1.0", "id": "1", "source": "es-1", "subject": "blue", "data": "blue"}
> {"specversion": "1.0", "id": "2", "source": "es-2", "subject": "yellow", "data": "yellow"}
> {"specversion": "1.0", "id": "3", "source": "es-3", "subject": "red", "data": "red"}
```

### Pulsar
```
EB=pulsar SLACK=https://hooks.slack.com/services/xxx go run ./...
```

Multiple instances can be run simultaneously. Run the following commands to produce test mesages:
```
bin/pulsar-client produce event -s ,, -m '{"specversion": "1.0", "id": "1", "source": "es-1", "subject": "blue", "data": "blue"}'
bin/pulsar-client produce event -s ,, -m '{"specversion": "1.0", "id": "2", "source": "es-2", "subject": "yellow", "data": "yellow"}'
bin/pulsar-client produce event -s ,, -m '{"specversion": "1.0", "id": "3", "source": "es-3", "subject": "red", "data": "red"}'
```
