package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"workflow-orchestrator/consumer"
	"workflow-orchestrator/models"
	"workflow-orchestrator/producer"
	"workflow-orchestrator/routes"
	"workflow-orchestrator/worker"

	"Shared/config"
	"Shared/logging"
	"Shared/metrics"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

func main() {
	cfg := config.Load()
	logger := logging.Init("workflow-orchestrator", cfg.LogLevel, cfg.LogFormat)

	db, err := gorm.Open(postgres.Open(cfg.PostgresDSN), &gorm.Config{
		Logger: gormlogger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags),
			gormlogger.Config{
				SlowThreshold:             time.Second,
				LogLevel:                  gormlogger.Warn,
				IgnoreRecordNotFoundError: true,
				Colorful:                  true,
			},
		),
	})
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	if err := db.AutoMigrate(
		&models.Workflow{},
		&models.Stage{},
		&models.WorkflowRun{},
		&models.StageExecution{},
	); err != nil {
		logger.Warn("migration warning", "error", err)
	}
	if err := models.MigrateStageIndex(db); err != nil {
		logger.Warn("stage index migration warning", "error", err)
	}

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	kafkaProducer := producer.InitKafkaProducer(cfg)
	kafkaConsumer := consumer.InitKafkaConsumer(cfg)
	dlqProducer := producer.InitDLQProducer(cfg)
	defer kafkaProducer.Close()
	defer kafkaConsumer.Close()
	defer dlqProducer.Close()
	defer redisClient.Close()

	metrics.InitMetricLabels()
	metricsSrv, err := metrics.StartServer(cfg.OrchestratorMetricsPort)
	if err != nil {
		logger.Error("metrics server failed", "error", err, "port", cfg.OrchestratorMetricsPort)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(shutdownCtx)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := worker.NewPool(cfg.WorkerPoolSize)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		worker.StartRedisPoller(ctx, cfg, db, redisClient, kafkaProducer, dlqProducer, pool)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		worker.ConsumeStageResult(ctx, cfg, kafkaConsumer, redisClient, db, dlqProducer, pool)
	}()

	router := routes.SetupRouter(db, redisClient)
	httpSrv := &http.Server{
		Addr:              fmt.Sprintf(":%s", cfg.HTTPPort),
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("http server listening", "addr", httpSrv.Addr, "worker_pool", cfg.WorkerPoolSize)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server error", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http shutdown", "error", err)
	}

	pool.Wait()
	wg.Wait()
	logger.Info("shutdown complete")
}
