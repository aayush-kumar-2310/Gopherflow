package main

import (
	"context"
	"fmt"
	"workflow-orchestrator/models"
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
	go worker.StartRedisPoller(context.Background(), db, redisClient)
	r := routes.SetupRouter(db, redisClient)
	r.Run(":8080")
}
