package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "something",
		"acks":              "all",
	})
	if err != nil {
		fmt.Printf("failed to create producer: %s\n", err)
	}

	deliverch := make(chan kafka.Event, 10000)
	topic := "HVSE"
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte("FOO"),
	},
		deliverch,
	)
	if err != nil {
		log.Fatal(err)
	}

	e := <-deliverch
	fmt.Printf("%+v\n", e.String())

	fmt.Printf("%+v\n", p)
}
