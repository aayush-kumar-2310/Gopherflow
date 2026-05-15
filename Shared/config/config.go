package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// loadDotEnvFromAncestors walks up from cwd to find a .env file (repo root when run from a subfolder).
func loadDotEnvFromAncestors() {
	dir, err := os.Getwd()
	if err != nil {
		LoadDotEnv(".env")
		return
	}
	for range 6 {
		candidate := filepath.Join(dir, ".env")
		if _, err := os.Stat(candidate); err == nil {
			LoadDotEnv(candidate)
			return
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	LoadDotEnv(".env")
}

// LoadDotEnv reads a .env file into the process environment (does not override existing vars).
func LoadDotEnv(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.Trim(strings.TrimSpace(val), `"`)
		if key != "" && os.Getenv(key) == "" {
			_ = os.Setenv(key, val)
		}
	}
}

// Config holds runtime settings loaded from environment variables.
type Config struct {
	PostgresDSN             string
	RedisAddr               string
	KafkaBrokers            []string
	KafkaExecuteTopic       string
	KafkaResponseTopic      string
	KafkaDLQTopic           string
	HTTPPort                string
	MetricsPort             string // deprecated: use service-specific ports
	OrchestratorMetricsPort string
	EventHandlerMetricsPort string
	WorkerPoolSize          int
	OllamaURL               string
	LogLevel                string
	LogFormat               string
	SMTPEnabled             bool
	SMTPHost                string
	SMTPPort                int
	SMTPUser                string
	SMTPPassword            string
	SMTPFrom                string
	SMTPTo                  string
	ShutdownTimeout         time.Duration
	KafkaReadTimeout        time.Duration
	RedisTimeout            time.Duration
	DBTimeout               time.Duration
}

func Load() Config {
	loadDotEnvFromAncestors()
	return Config{
		PostgresDSN:             getEnv("POSTGRES_DSN", "host=localhost user=aayush password= dbname=gopherflow port=5432 sslmode=disable"),
		RedisAddr:               getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:            splitCSV(getEnv("KAFKA_BROKERS", "localhost:9092")),
		KafkaExecuteTopic:       getEnv("KAFKA_EXECUTE_TOPIC", "execute-stage"),
		KafkaResponseTopic:      getEnv("KAFKA_RESPONSE_TOPIC", "execution-response"),
		KafkaDLQTopic:           getEnv("KAFKA_DLQ_TOPIC", "stage-dlq"),
		HTTPPort:                getEnv("HTTP_PORT", "8080"),
		MetricsPort:             getEnv("METRICS_PORT", "9091"),
		OrchestratorMetricsPort: getEnv("ORCHESTRATOR_METRICS_PORT", getEnv("METRICS_PORT", "9091")),
		// Default 9094 — 9092 is the usual Kafka broker port and must not be reused for HTTP metrics.
		EventHandlerMetricsPort: getEnv("EVENT_HANDLER_METRICS_PORT", "9094"),
		WorkerPoolSize:          getEnvInt("WORKER_POOL_SIZE", 16),
		OllamaURL:               getEnv("OLLAMA_URL", "http://localhost:11434/api/generate"),
		LogLevel:                getEnv("LOG_LEVEL", "info"),
		LogFormat:               getEnv("LOG_FORMAT", "json"),
		SMTPEnabled:             getEnvBool("SMTP_ENABLED", false),
		SMTPHost:                getEnv("SMTP_HOST", "localhost"),
		SMTPPort:                getEnvInt("SMTP_PORT", 587),
		SMTPUser:                getEnv("SMTP_USER", ""),
		SMTPPassword:            getEnv("SMTP_PASSWORD", ""),
		SMTPFrom:                getEnv("SMTP_FROM", "gopherflow@localhost"),
		SMTPTo:                  getEnv("SMTP_TO", ""),
		ShutdownTimeout:         time.Duration(getEnvInt("SHUTDOWN_TIMEOUT_SEC", 15)) * time.Second,
		KafkaReadTimeout:        time.Duration(getEnvInt("KAFKA_READ_TIMEOUT_SEC", 5)) * time.Second,
		RedisTimeout:            time.Duration(getEnvInt("REDIS_TIMEOUT_SEC", 3)) * time.Second,
		DBTimeout:               time.Duration(getEnvInt("DB_TIMEOUT_SEC", 10)) * time.Second,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getEnvBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
