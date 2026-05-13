package main

import (
	"Event_Handler/consumer"
	"Event_Handler/executors"
	"Event_Handler/producer"
)

func main() {
	consumer := consumer.InitKafkaConsumer()
	producer := producer.InitKafkaProducer()
	go executors.ConsumeExecuteStageMessages(consumer, producer)
	defer consumer.Close()
	defer producer.Close()
}
