package main

import (
	"Event_Handler/consumer"
	"Event_Handler/executors"
	"Event_Handler/producer"
	ehworker "Event_Handler/worker"
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"Shared/config"
	"Shared/logging"
	"Shared/metrics"
)

func main() {
	cfg := config.Load()
	logger := logging.Init("event-handler", cfg.LogLevel, cfg.LogFormat)
	executors.Init(cfg)

	kafkaConsumer := consumer.InitKafkaConsumer(cfg)
	kafkaProducer := producer.InitKafkaProducer(cfg)
	dlqProducer := producer.InitDLQProducer(cfg)
	defer kafkaConsumer.Close()
	defer kafkaProducer.Close()
	defer dlqProducer.Close()

	metrics.InitMetricLabels()
	metricsSrv, err := metrics.StartServer(cfg.EventHandlerMetricsPort)
	if err != nil {
		logger.Error("metrics server failed", "error", err, "port", cfg.EventHandlerMetricsPort)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(shutdownCtx)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := ehworker.NewPool(cfg.WorkerPoolSize)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("event handler started", "worker_pool", cfg.WorkerPoolSize)
		executors.ConsumeExecuteStageMessages(ctx, cfg, kafkaConsumer, kafkaProducer, dlqProducer, pool)
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	pool.Wait()
	wg.Wait()
	logger.Info("shutdown complete")
}
