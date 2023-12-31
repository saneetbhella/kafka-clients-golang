package client

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

type kafkaClient struct {
	client   *kafka.Consumer
	registry schemaregistry.Client
}

type Client interface {
	GetKafkaConsumer() *kafka.Consumer
	GetSchemaRegistry() schemaregistry.Client
	NewDeserializer() (*avro.SpecificDeserializer, error)
}

func New(kafkaConfig kafka.ConfigMap) (Client, error) {
	client, err := kafka.NewConsumer(&kafkaConfig)
	if err != nil {
		return nil, err
	}

	registry, err := schemaregistry.NewClient(schemaregistry.NewConfig("http://localhost:8081"))
	if err != nil {
		return nil, err
	}

	return &kafkaClient{
		client:   client,
		registry: registry,
	}, nil
}

func (c *kafkaClient) GetKafkaConsumer() *kafka.Consumer {
	return c.client
}

func (c *kafkaClient) GetSchemaRegistry() schemaregistry.Client {
	return c.registry
}

func (c *kafkaClient) NewDeserializer() (*avro.SpecificDeserializer, error) {
	d, err := avro.NewSpecificDeserializer(c.registry, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		return nil, err
	}
	return d, nil
}
