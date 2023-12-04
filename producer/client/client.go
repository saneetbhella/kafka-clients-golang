package client

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

type kafkaClient struct {
	client   *kafka.Producer
	registry schemaregistry.Client
}

type Client interface {
	GetKafkaProducer() *kafka.Producer
	GetSchemaRegistry() schemaregistry.Client
	NewSerializer() (*avro.SpecificSerializer, error)
}

func New(kafkaConfig kafka.ConfigMap) (Client, error) {
	client, err := kafka.NewProducer(&kafkaConfig)
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

func (c *kafkaClient) GetKafkaProducer() *kafka.Producer {
	return c.client
}

func (c *kafkaClient) GetSchemaRegistry() schemaregistry.Client {
	return c.registry
}

func (c *kafkaClient) NewSerializer() (*avro.SpecificSerializer, error) {
	d, err := avro.NewSpecificSerializer(c.registry, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		return nil, err
	}
	return d, nil
}
