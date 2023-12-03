# Kafka Clients Golang

## Generating models from Avro schema

Install Avro tool globally:

```bash
go install github.com/actgardner/gogen-avro/v10/cmd/...@latest
```

Run command to generate model from Avro file:

```bash
gogen-avro -package model ./model ./avsc/payment.avsc
```

## Kafka Consumer

### Building

To build the consumer:

```bash

```

### Running

To run the consumer:

```bash
go run consumer/consumer.go
```
