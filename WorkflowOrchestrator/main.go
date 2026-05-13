package main

import (
	"context"
	"fmt"
	"workflow-orchestrator/consumer"
	"workflow-orchestrator/models"
	"workflow-orchestrator/producer"
	"workflow-orchestrator/routes"
	"workflow-orchestrator/worker"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func initDB() *gorm.DB {
	dsn := "host=localhost user=aayush dbname=gopherflow port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})

	if err != nil {
		panic("Failed to connect to database!")
	}

	err = db.AutoMigrate(&models.Workflow{}, &models.Stage{})
	if err != nil {
		fmt.Printf("Migration failed: %v\n", err)
	}
	return db
}

func initRedis() *redis.Client {
	var RedisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	return RedisClient
}

func main() {
	db := initDB()
	redisClient := initRedis()
	kafkaProducer := producer.InitKafkaProducer()
	kafkaConsumer := consumer.InitKafkaConsumer()
	defer kafkaProducer.Close()
	defer kafkaConsumer.Close()
	go worker.StartRedisPoller(context.Background(), db, redisClient, kafkaProducer)
	go worker.ConsumeStageResult(kafkaConsumer)
	r := routes.SetupRouter(db, redisClient)
	r.Run(":8080")
}
