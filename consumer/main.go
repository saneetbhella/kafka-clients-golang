package main

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/saneetbhella/logger"
	"kafka-clients-golang/consumer/client"
	"kafka-clients-golang/model"
)

func main() {
	config := kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "kafka-consumer-golang",
		"auto.offset.reset": "earliest",

		"enable.auto.commit": false,

		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",

		"sasl.username": "admin",
		"sasl.password": "admin-secret",

		"ssl.ca.location": "/Users/SaneetBhella/Projects/kafka-docker/certs/kafka/ca.pem",
	}

	c, err := client.New(config)
	if err != nil {
		logger.Error("Could not create Kafka client", err)
		panic(err)
	}

	kafkaConsumer := c.GetKafkaConsumer()

	deserializer, err := c.NewDeserializer()
	if err != nil {
		logger.Error("Could not create Avro deserializer", err)
		panic(err)
	}

	if err := kafkaConsumer.Subscribe("payments", nil); err != nil {
		logger.Error("Failed to subscribe to topic")
		panic(err)
	}

	defer func() {
		if err := kafkaConsumer.Close(); err != nil {
			logger.Error("Failed to properly close Kafka connection")
			panic(err)
		}
	}()

	for {
		ev := kafkaConsumer.Poll(100)

		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			value := model.Payment{}
			err := deserializer.DeserializeInto(*e.TopicPartition.Topic, e.Value, &value)
			if err != nil {
				logger.Error("Failed to deserialize message", err)
			} else {
				logger.Infof("Key: %s, Value: %s", string(e.Key), value)
				_, err := kafkaConsumer.Commit()
				if err != nil {
					logger.Error("Failed to commit offset", err)
				}
			}
		case kafka.Error:
			logger.Error(e.Error())
		default:
			logger.Warnf("Ignored %v\n", e)
		}
	}
}
