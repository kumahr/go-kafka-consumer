package main

import (
	"context"
	"log"

	"gopkg.in/Shopify/sarama.v1"
)

type KafkaHandler struct{}

func (KafkaHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (KafkaHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (KafkaHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		session.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	config.Consumer.Return.Errors = true
	handler := KafkaHandler{}
	cg, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "doge", config)
	if err != nil {
		panic(err)
	}
	for {
		err := cg.Consume(context.Background(), []string{"topic_in"}, handler)
		if err != nil {
			panic(err)
		}
	}
}
