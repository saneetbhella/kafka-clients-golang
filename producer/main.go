package main

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/google/uuid"
	"github.com/saneetbhella/logger"
	"kafka-clients-golang/model"
	"kafka-clients-golang/producer/client"
)

func main() {
	topic := "payments"

	config := kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",

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

	kafkaProducer := c.GetKafkaProducer()
	defer kafkaProducer.Close()

	serializer, err := c.NewSerializer()
	if err != nil {
		logger.Error("Could not create Avro serializer", err)
		panic(err)
	}

	for i := 0; i < 1000; i++ {
		if err := produceMessage(kafkaProducer, serializer, topic); err != nil {
			logger.Error("Error producing message", err)
			panic(err)
		}
	}

	kafkaProducer.Flush(100)

	logger.Info("Produced messages")
}

func produceMessage(kafkaProducer *kafka.Producer, serializer *avro.SpecificSerializer, topic string) error {
	newUuid := uuid.New().String()

	value := model.Payment{
		Id:          newUuid,
		Description: "A description",
	}

	payload, err := serializer.Serialize(topic, &value)
	if err != nil {
		logger.Error("Could not serialize data", err)
		return err
	}

	err = kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(newUuid),
		Value:          payload,
	}, nil)

	if err != nil {
		return err
	}

	return nil
}
