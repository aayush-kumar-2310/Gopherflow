package metrics

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	StagesDispatched = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gopherflow_stages_dispatched_total",
		Help: "Stages dispatched to Kafka for execution",
	}, []string{"service"})

	StagesCompleted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gopherflow_stages_completed_total",
		Help: "Stage execution outcomes",
	}, []string{"service", "status"})

	StageRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gopherflow_stage_retries_total",
		Help: "Stage retries scheduled",
	}, []string{"service"})

	DLQPublished = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gopherflow_dlq_messages_total",
		Help: "Messages published to the dead-letter topic",
	}, []string{"service", "reason"})

	WorkflowRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gopherflow_workflow_runs_total",
		Help: "Workflow runs started or finalized",
	}, []string{"status"})

	KafkaConsumed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gopherflow_kafka_messages_consumed_total",
		Help: "Kafka messages consumed",
	}, []string{"service", "topic"})

	KafkaProduced = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gopherflow_kafka_messages_produced_total",
		Help: "Kafka messages produced",
	}, []string{"service", "topic"})

	StageDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gopherflow_stage_execution_seconds",
		Help:    "Wall-clock time to execute a stage in Event_Handler",
		Buckets: prometheus.DefBuckets,
	}, []string{"stage_type"})

	WorkerPoolInFlight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gopherflow_worker_pool_in_flight",
		Help: "Tasks currently running in the worker pool",
	}, []string{"service"})
)

func init() {
	prometheus.MustRegister(
		StagesDispatched,
		StagesCompleted,
		StageRetries,
		DLQPublished,
		WorkflowRuns,
		KafkaConsumed,
		KafkaProduced,
		StageDuration,
		WorkerPoolInFlight,
	)
}

// StartServer exposes Prometheus metrics on /metrics. Fails immediately if the port is in use.
func StartServer(port string) (*http.Server, error) {
	ln, err := net.Listen("tcp", net.JoinHostPort("", port))
	if err != nil {
		return nil, fmt.Errorf("metrics listen on :%s: %w (9092 is usually Kafka — use EVENT_HANDLER_METRICS_PORT=9094)", port, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		slog.Info("prometheus metrics listening", "addr", ln.Addr().String())
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server failed", "error", err)
		}
	}()
	return srv, nil
}

// FetchText returns /metrics body or an error message.
func FetchText(port string) (string, error) {
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/metrics", port))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metrics HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func InitMetricLabels() {
	// Orchestrator
	StagesDispatched.
		WithLabelValues("workflow-orchestrator").
		Add(0)

	KafkaProduced.
		WithLabelValues("workflow-orchestrator", "execute-stage").
		Add(0)

	WorkflowRuns.
		WithLabelValues("started").
		Add(0)

	WorkflowRuns.
		WithLabelValues("completed").
		Add(0)

	// Event Handler
	StagesCompleted.
		WithLabelValues("event-handler", "success").
		Add(0)

	StagesCompleted.
		WithLabelValues("event-handler", "failure").
		Add(0)

	StageRetries.
		WithLabelValues("event-handler").
		Add(0)

	DLQPublished.
		WithLabelValues("event-handler", "retry_exhausted").
		Add(0)

	KafkaConsumed.
		WithLabelValues("event-handler", "execute-stage").
		Add(0)

	KafkaProduced.
		WithLabelValues("event-handler", "execution-response").
		Add(0)

	WorkerPoolInFlight.
		WithLabelValues("workflow-orchestrator").
		Set(0)

	WorkerPoolInFlight.
		WithLabelValues("event-handler").
		Set(0)
}
