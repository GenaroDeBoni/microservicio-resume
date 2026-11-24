package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config es la configuración validada en tiempo de ejecución del microservicio de resumen.
type Config struct {
	Port          string
	AppName       string
	AppVersion    string
	LogLevel      string
	TZ            string

	// Kafka
	KafkaBrokers      []string
	KafkaTopicValidated string
	KafkaTopicCompleted string
	KafkaTopicDLQ       string
	KafkaConsumerGroup string
	KafkaClientID     string

	// IA (proveedor OpenAI-compatible)
	AIAPIKey   string
	AIBaseURL  string
	AIModel    string
	AIMaxTokens int
	AITemperature float64
	AITimeout   time.Duration
	AIMaxRetries int
	AIBackoffBase time.Duration

	// PDF
	MaxPages int

	// Persistencia vía HTTP al microservicio-db-go
	DBServiceURL     string
	DBServiceTimeout time.Duration
	DBApiKey         string // opcional: autenticación Bearer contra db-go
	// El evento pdf.validated se publica antes de persistir, por eso el worker
	// espera a que el documento exista antes de dar por fallido el resumen.
	DBWaitTimeout  time.Duration
	DBWaitInterval time.Duration

	// RFC 9457
	ErrBaseURL string
}

const (
	defaultPort             = "8080"
	defaultAppName          = "microservicio-resume"
	defaultAppVersion       = "0.1.0"
	defaultLogLevel         = "INFO"
	defaultTZ               = "America/Argentina/Buenos_Aires"

	defaultKafkaConsumerGroup = "resume-worker"
	defaultKafkaClientID      = "microservicio-resume"
	defaultKafkaTopicValidated = "pdf.validated"
	defaultKafkaTopicCompleted = "summary.completed"
	defaultKafkaTopicDLQ       = "summary.dlq"

	defaultAIModel     = "gpt-4o-mini"
	defaultAIMaxTokens = 500
	defaultAITemperature = 0.3
	defaultAITimeout   = 30 * time.Second
	defaultAIMaxRetries = 3
	defaultAIBackoffBase = 1 * time.Second

	defaultMaxPages = 10

	defaultDBServiceTimeout = 10 * time.Second
	defaultDBWaitTimeout    = 2 * time.Minute
	defaultDBWaitInterval   = 1 * time.Second
	defaultErrBaseURL       = "https://errors.example.com"
)

// Load lee y valida el entorno. Falla con un mensaje accionable nombrando la variable infractora.
func Load() (*Config, error) {
	cfg := &Config{
		Port:              getEnv("PORT", defaultPort),
		AppName:           getEnv("APP_NAME", defaultAppName),
		AppVersion:        getEnv("APP_VERSION", defaultAppVersion),
		LogLevel:          getEnv("LOG_LEVEL", defaultLogLevel),
		TZ:                getEnv("TZ", defaultTZ),
		KafkaConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", defaultKafkaConsumerGroup),
		KafkaClientID:     getEnv("KAFKA_CLIENT_ID", defaultKafkaClientID),
		KafkaTopicValidated: getEnv("KAFKA_TOPIC_VALIDATED", defaultKafkaTopicValidated),
		KafkaTopicCompleted: getEnv("KAFKA_TOPIC_COMPLETED", defaultKafkaTopicCompleted),
		KafkaTopicDLQ:       getEnv("KAFKA_TOPIC_DLQ", defaultKafkaTopicDLQ),
		AIModel:             getEnv("AI_MODEL", defaultAIModel),
		AIMaxTokens:         defaultAIMaxTokens,
		AITemperature:       defaultAITemperature,
		AITimeout:           defaultAITimeout,
		AIMaxRetries:        defaultAIMaxRetries,
		AIBackoffBase:       defaultAIBackoffBase,
		MaxPages:            defaultMaxPages,
		DBServiceTimeout:    defaultDBServiceTimeout,
		DBWaitTimeout:       defaultDBWaitTimeout,
		DBWaitInterval:      defaultDBWaitInterval,
		ErrBaseURL:          getEnv("ERR_BASE_URL", defaultErrBaseURL),
	}

	// Kafka brokers (requerido, lista separada por comas)
	brokersRaw := os.Getenv("KAFKA_BROKERS")
	if brokersRaw == "" {
		return nil, fmt.Errorf("KAFKA_BROKERS es requerido")
	}
	for _, b := range strings.Split(brokersRaw, ",") {
		b = strings.TrimSpace(b)
		if b != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, b)
		}
	}
	if len(cfg.KafkaBrokers) == 0 {
		return nil, fmt.Errorf("KAFKA_BROKERS debe contener al menos un broker")
	}

	// IA API key (requerido)
	cfg.AIAPIKey = os.Getenv("AI_API_KEY")
	if cfg.AIAPIKey == "" {
		return nil, fmt.Errorf("AI_API_KEY es requerida")
	}
	cfg.AIBaseURL = getEnv("AI_BASE_URL", "https://api.openai.com/v1")
	if !strings.HasPrefix(cfg.AIBaseURL, "http://") && !strings.HasPrefix(cfg.AIBaseURL, "https://") {
		return nil, fmt.Errorf("AI_BASE_URL debe ser una URI http(s), recibido %q", cfg.AIBaseURL)
	}

	// IA MAX_TOKENS
	if raw := os.Getenv("AI_MAX_TOKENS"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("AI_MAX_TOKENS debe ser un entero positivo, recibido %q", raw)
		}
		cfg.AIMaxTokens = v
	}

	// IA TEMPERATURE
	if raw := os.Getenv("AI_TEMPERATURE"); raw != "" {
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || v < 0 || v > 2 {
			return nil, fmt.Errorf("AI_TEMPERATURE debe ser un número entre 0 y 2, recibido %q", raw)
		}
		cfg.AITemperature = v
	}

	// IA TIMEOUT
	if raw := os.Getenv("AI_TIMEOUT"); raw != "" {
		v, err := time.ParseDuration(raw)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("AI_TIMEOUT debe ser una duración positiva (ej. 30s), recibido %q", raw)
		}
		cfg.AITimeout = v
	}

	// IA MAX_RETRIES
	if raw := os.Getenv("AI_MAX_RETRIES"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 0 {
			return nil, fmt.Errorf("AI_MAX_RETRIES debe ser un entero no negativo, recibido %q", raw)
		}
		cfg.AIMaxRetries = v
	}

	// IA BACKOFF_BASE
	if raw := os.Getenv("AI_BACKOFF_BASE"); raw != "" {
		v, err := time.ParseDuration(raw)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("AI_BACKOFF_BASE debe ser una duración positiva (ej. 1s), recibido %q", raw)
		}
		cfg.AIBackoffBase = v
	}

	// MAX_PAGES
	if raw := os.Getenv("MAX_PAGES"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("MAX_PAGES debe ser un entero positivo, recibido %q", raw)
		}
		cfg.MaxPages = v
	}

	// DB_SERVICE_URL (requerido)
	cfg.DBServiceURL = os.Getenv("DB_SERVICE_URL")
	if cfg.DBServiceURL == "" {
		return nil, fmt.Errorf("DB_SERVICE_URL es requerida")
	}
	if !strings.HasPrefix(cfg.DBServiceURL, "http://") && !strings.HasPrefix(cfg.DBServiceURL, "https://") {
		return nil, fmt.Errorf("DB_SERVICE_URL debe ser una URI http(s), recibido %q", cfg.DBServiceURL)
	}
	cfg.DBServiceURL = strings.TrimSuffix(cfg.DBServiceURL, "/")

	// DB_SERVICE_TIMEOUT
	if raw := os.Getenv("DB_SERVICE_TIMEOUT"); raw != "" {
		v, err := time.ParseDuration(raw)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("DB_SERVICE_TIMEOUT debe ser una duración positiva (ej. 10s), recibido %q", raw)
		}
		cfg.DBServiceTimeout = v
	}

	// DB_WAIT_TIMEOUT / DB_WAIT_INTERVAL
	if raw := os.Getenv("DB_WAIT_TIMEOUT"); raw != "" {
		v, err := time.ParseDuration(raw)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("DB_WAIT_TIMEOUT debe ser una duración positiva (ej. 2m), recibido %q", raw)
		}
		cfg.DBWaitTimeout = v
	}
	if raw := os.Getenv("DB_WAIT_INTERVAL"); raw != "" {
		v, err := time.ParseDuration(raw)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("DB_WAIT_INTERVAL debe ser una duración positiva (ej. 1s), recibido %q", raw)
		}
		cfg.DBWaitInterval = v
	}

	// DB_API_KEY (opcional)
	cfg.DBApiKey = os.Getenv("DB_API_KEY")

	// ERR_BASE_URL
	if raw := os.Getenv("ERR_BASE_URL"); raw != "" {
		if len(raw) < 8 || (raw[:7] != "http://" && raw[:8] != "https://") {
			return nil, fmt.Errorf("ERR_BASE_URL debe ser una URI http(s), recibido %q", raw)
		}
		cfg.ErrBaseURL = raw
	}

	// Puerto numérico
	if _, err := strconv.Atoi(cfg.Port); err != nil {
		return nil, fmt.Errorf("PORT debe ser numérico, recibido %q", cfg.Port)
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}