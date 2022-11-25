package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	c := NewKafkaConsumer("teste")

	for {
		msg, err1 := c.ReadMessage(-1)
		if err1 == nil {
			fmt.Println(string(msg.Value), msg.TopicPartition)
		}
	}
}

func NewKafkaConsumer(topic string) *kafka.Consumer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "fullcycle_kafka_go_kafka_1:9092",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
		"auto.offset.reset": "earliest",
	}
	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		fmt.Println("error consumer", err.Error())
	}
	topics := []string{topic}
	c.SubscribeTopics(topics, nil)
	return c
}
