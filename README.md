# Kafka Clients Golang

This repository contains a basic Kafka Consumer & Producer written in Golang using the Confluent Kafka library. This can
be used with [Kafka Docker](https://github.com/saneetbhella/kafka-docker).

## Generating models from Avro schema

Install Avro tool globally:

```bash
go install github.com/actgardner/gogen-avro/v10/cmd/...@latest
```

Run command to generate model from Avro file:

```bash
gogen-avro -package model ./model ./avsc/payment.avsc
```

## Install Dependencies

```bash
go get ./...
```

## Build

```bash
go build ./...
```

## Running

### Kafka Consumer

```bash
go run consumer/main.go
```

### Kafka Consumer

```bash
go run producer/main.go
```
