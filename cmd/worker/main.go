package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"microservicio-resume/application"
	"microservicio-resume/config"
	"microservicio-resume/infrastructure/ai"
	dbclient "microservicio-resume/infrastructure/http"
	"microservicio-resume/infrastructure/kafka"
	"microservicio-resume/presentation"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run() error {
	// Logging estructurado (JSON) para observabilidad en contenedores.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("no se pudo cargar la configuración", slog.String("error", err.Error()))
		return fmt.Errorf("cargar config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── Dependencias (Inyección explícita) ──
	summarizer := ai.NewProvider(
		cfg.AIBaseURL, cfg.AIAPIKey, cfg.AIModel,
		cfg.AIMaxTokens, cfg.AITemperature, cfg.AITimeout,
		cfg.AIMaxRetries, cfg.AIBackoffBase,
	)
	dbClient := dbclient.NewClientWithWait(
		cfg.DBServiceURL,
		cfg.DBServiceTimeout,
		cfg.DBWaitTimeout,
		cfg.DBWaitInterval,
		cfg.DBApiKey,
	)

	useCase := application.NewGenerateSummaryUseCase(
		summarizer, dbClient,
	)

	// Clientes Kafka reales (segmentio/kafka-go).
	reader, err := kafka.NewKafkaReader(kafka.ReaderOptions{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.KafkaTopicValidated,
		GroupID:  cfg.KafkaConsumerGroup,
		ClientID: cfg.KafkaClientID,
	})
	if err != nil {
		return fmt.Errorf("crear lector Kafka: %w", err)
	}
	defer func() { _ = reader.Close() }()

	writer, err := kafka.NewKafkaWriter(kafka.WriterOptions{
		Brokers:  cfg.KafkaBrokers,
		ClientID: cfg.KafkaClientID,
	})
	if err != nil {
		return fmt.Errorf("crear escritor Kafka: %w", err)
	}
	defer func() { _ = writer.Close() }()

	consumer := kafka.NewConsumer(
		reader, writer, useCase,
		cfg.KafkaTopicValidated, cfg.KafkaTopicCompleted, cfg.KafkaTopicDLQ,
	)

	// ── Servidor HTTP interno (healthcheck únicamente, no expuesto por Traefik) ──
	router := presentation.NewRouter(presentation.Config{ErrBaseURL: cfg.ErrBaseURL})
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)

	// Goroutine: servidor HTTP (healthcheck)
	go func() {
		slog.Info("servidor de salud escuchando", slog.String("port", cfg.Port))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("servidor HTTP: %w", err)
		}
	}()

	// Goroutine: consumidor de Kafka (worker principal)
	go func() {
		slog.Info("consumidor Kafka iniciado", slog.String("topic", cfg.KafkaTopicValidated))
		if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("consumidor Kafka: %w", err)
		}
	}()

	// Esperar señal de apagado o error
	select {
	case err := <-errCh:
		return fmt.Errorf("runtime: %w", err)
	case <-ctx.Done():
		slog.Info("señal de apagado recibida")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("apagado HTTP falló", slog.String("error", err.Error()))
			return fmt.Errorf("apagado HTTP: %w", err)
		}
		slog.Info("apagado completo")
		return nil
	}