package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"microservicio-resume/application"
	"microservicio-resume/domain"
)

// Message representa un mensaje de Kafka de forma agnóstica al cliente concreto.
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Offset    int64
	Partition int32
}

// Reader es la interfaz para consumir mensajes de Kafka.
type Reader interface {
	FetchMessage(ctx context.Context) (Message, error)
	CommitMessages(ctx context.Context, msgs ...Message) error
	Close() error
}

// Writer es la interfaz para producir mensajes a Kafka.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...Message) error
	Close() error
}

// UseCase es la interfaz que el consumidor necesita para generar resúmenes.
type UseCase interface {
	Execute(ctx context.Context, req application.SummarizeRequest) (*application.SummarizeResponse, error)
}

// validatedEvent es el evento de entrada pdf.validated.
type validatedEvent struct {
	DocumentID   string `json:"document_id"`
	Filename     string `json:"filename"`
	Checksum     string `json:"checksum,omitempty"`
	FileSizeBytes int64  `json:"file_size_bytes,omitempty"`
	ValidatedAt  string `json:"validated_at,omitempty"`
}

// completedEvent es el evento de salida summary.completed.
type completedEvent struct {
	DocumentID   string    `json:"document_id"`
	Summary      string    `json:"summary"`
	Model        string    `json:"model"`
	CompletedAt  time.Time `json:"completed_at"`
	Status       string    `json:"status"`
}

// dlqEvent es el evento de Dead Letter Queue.
type dlqEvent struct {
	DocumentID   string    `json:"document_id"`
	Error        string    `json:"error"`
	RetryCount   int       `json:"retry_count"`
	FailedAt     time.Time `json:"failed_at"`
	OriginalEvent json.RawMessage `json:"original_event"`
}

// Consumer consume eventos de pdf.validated y orquesta la generación de resumen.
type Consumer struct {
	reader         Reader
	writer         Writer
	useCase        UseCase
	topicValidated   string
	topicCompleted   string
	topicDLQ         string
}

// NewConsumer construye un consumidor de Kafka con sus dependencias inyectadas.
func NewConsumer(reader Reader, writer Writer, useCase UseCase, topicValidated, topicCompleted, topicDLQ string) *Consumer {
	return &Consumer{
		reader:         reader,
		writer:         writer,
		useCase:        useCase,
		topicValidated: topicValidated,
		topicCompleted: topicCompleted,
		topicDLQ:       topicDLQ,
	}
}

// ProcessMessage procesa un único mensaje de Kafka.
// Flujo:
//  1. Parsear evento pdf.validated
//  2. Ejecutar caso de uso GenerateSummaryUseCase
//  3. Si éxito: publicar summary.completed y hacer commit
//  4. Si error de idempotencia: solo hacer commit (no reintentar, no DLQ)
//  5. Si otro error: publicar a summary.dlq y hacer commit
func (c *Consumer) ProcessMessage(ctx context.Context, msg Message) error {
	// Paso 1: Parsear evento
	var evt validatedEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		return fmt.Errorf("parsear evento pdf.validated: %w", err)
	}

	// Paso 2: Ejecutar caso de uso de resumen
	// El texto se obtiene internamente desde microservicio-db-go dentro del caso de uso.
	req := application.SummarizeRequest{
		DocumentID: evt.DocumentID,
		Filename:   evt.Filename,
	}
	resp, err := c.useCase.Execute(ctx, req)
	if err != nil {
		// Idempotencia: si ya existe resumen, no es error crítico, solo commit
		if errors.Is(err, domain.ErrSummaryAlreadyExists) {
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				return fmt.Errorf("commit tras idempotencia: %w", err)
			}
			return nil
		}

		// Otros errores: enviar a DLQ
		if perr := c.publishDLQ(ctx, msg, evt.DocumentID, err); perr != nil {
			return fmt.Errorf("publicar DLQ: %w; error original: %v", perr, err)
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit tras DLQ: %w", err)
		}
		return nil
	}

	// Paso 3: Éxito - publicar summary.completed
	if perr := c.publishCompleted(ctx, evt.DocumentID, resp.Summary, resp.Model); perr != nil {
		return fmt.Errorf("publicar summary.completed: %w", perr)
	}

	// Paso 4: Commit del mensaje procesado
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("commit tras éxito: %w", err)
	}

	return nil
}

// publishCompleted publica un evento summary.completed en Kafka.
func (c *Consumer) publishCompleted(ctx context.Context, documentID, summary, model string) error {
	evt := completedEvent{
		DocumentID:  documentID,
		Summary:     summary,
		Model:       model,
		CompletedAt: time.Now().UTC(),
		Status:      "completed",
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marcar evento completed: %w", err)
	}
	return c.writer.WriteMessages(ctx, Message{
		Topic: c.topicCompleted,
		Key:   []byte(documentID),
		Value: payload,
	})
}

// publishDLQ publica un evento a la Dead Letter Queue.
func (c *Consumer) publishDLQ(ctx context.Context, msg Message, documentID string, err error) error {
	evt := dlqEvent{
		DocumentID:  documentID,
		Error:       err.Error(),
		RetryCount:  0,
		FailedAt:    time.Now().UTC(),
		OriginalEvent: msg.Value,
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marcar evento DLQ: %w", err)
	}
	return c.writer.WriteMessages(ctx, Message{
		Topic: c.topicDLQ,
		Key:   msg.Key,
		Value: payload,
	})
}

// Run inicia el bucle de consumo (se llama desde cmd/worker/main.go).
func (c *Consumer) Run(ctx context.Context) error {
	consecutiveErrors := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			consecutiveErrors++
			slog.Warn("kafka: error transitorio al leer",
				slog.String("error", err.Error()),
				slog.Int("consecutive_errors", consecutiveErrors),
			)
			delay := BackoffDelay(consecutiveErrors-1, kafkaBackoffBase)
			if !sleepContext(ctx, delay) {
				return ctx.Err()
			}
			continue
		}

		if perr := c.ProcessMessage(ctx, msg); perr != nil {
			// El mensaje puede no haber sido commiteado (ej. JSON inválido);
			// se reintenta con backoff exponencial para no girar en caliente.
			consecutiveErrors++
			slog.Warn("kafka: error al procesar mensaje",
				slog.String("error", perr.Error()),
				slog.Int("consecutive_errors", consecutiveErrors),
			)
			delay := BackoffDelay(consecutiveErrors-1, kafkaBackoffBase)
			if !sleepContext(ctx, delay) {
				return ctx.Err()
			}
			continue
		}

		// Éxito: resetear contador de errores consecutivos.
		consecutiveErrors = 0
	}
}

// kafkaBackoffBase es la duración base para el backoff exponencial del bucle de consumo.
const kafkaBackoffBase = time.Second

// BackoffDelay devuelve el tiempo de espera para el intento dado (0-indexado) usando
// backoff exponencial: base, base*2, base*4, base*8...
func BackoffDelay(attempt int, base time.Duration) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	return base * (1 << attempt)
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}