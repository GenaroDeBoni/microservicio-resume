package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"microservicio-resume/domain"
)

// chatCompletionRequest es el payload enviado a la API OpenAI-compatible.
type chatCompletionRequest struct {
	Model       string    `json:"model"`
	Messages    []message `json:"messages"`
	MaxTokens   int       `json:"max_tokens"`
	Temperature float64   `json:"temperature"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// chatCompletionResponse es la respuesta de la API OpenAI-compatible.
type chatCompletionResponse struct {
	Choices []choice `json:"choices"`
}

type choice struct {
	Message message `json:"message"`
}

// systemPrompt es el prompt de sistema enfocado en síntesis estructurada.
const systemPrompt = `Eres un asistente experto en síntesis de documentos. Recibirás el texto extraído
de las primeras páginas de un PDF. Tu tarea es generar un resumen estructurado, conciso y
claro que capture las ideas principales. Ignora referencias a imágenes, gráficos o metadatos
que no aporten contenido textual. Responde únicamente con el resumen, sin preámbulos.`

// Provider implementa domain.SummarizerProvider llamando a una API OpenAI-compatible
// mediante HTTP. Es agnóstico al proveedor concreto (OpenAI, Azure, local, etc.)
// mediante AI_BASE_URL y AI_API_KEY configurables por entorno.
type Provider struct {
	baseURL    string
	apiKey     string
	model      string
	maxTokens  int
	temperature float64
	timeout    time.Duration
	maxRetries int
	backoffBase time.Duration
	client     *http.Client
}

// NewProvider construye un Provider con sus dependencias inyectadas.
func NewProvider(baseURL, apiKey, model string, maxTokens int, temperature float64, timeout time.Duration, maxRetries int, backoffBase time.Duration) *Provider {
	if maxRetries < 0 {
		maxRetries = 0
	}
	if backoffBase <= 0 {
		backoffBase = 1 * time.Second
	}
	return &Provider{
		baseURL:     baseURL,
		apiKey:      apiKey,
		model:       model,
		maxTokens:   maxTokens,
		temperature: temperature,
		timeout:     timeout,
		maxRetries:  maxRetries,
		backoffBase: backoffBase,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// Model devuelve el modelo configurado, usado para trazabilidad en los eventos.
func (p *Provider) Model() string {
	return p.model
}

// Summarize envía el texto al proveedor de IA y devuelve el resumen generado.
// Reintenta con backoff exponencial ante rate limits (429). No reintenta ante
// errores 5xx (salvo 429) ni ante errores de cliente (4xx).
func (p *Provider) Summarize(ctx context.Context, text string) (string, error) {
	body, err := p.buildRequest(text)
	if err != nil {
		return "", fmt.Errorf("construir request: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		if attempt > 0 {
			// Backoff exponencial: base * 2^(attempt-1)
			delay := p.backoffBase * (1 << (attempt - 1))
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
			}
		}

		result, retryable, err := p.doRequest(ctx, body)
		if err != nil {
			lastErr = err
			if !retryable {
				return "", err
			}
			continue
		}
		return result, nil
	}

	return "", domain.NewIAProviderError(fmt.Sprintf("agotados %d reintentos: %v", p.maxRetries, lastErr))
}

func (p *Provider) buildRequest(text string) ([]byte, error) {
	req := chatCompletionRequest{
		Model:       p.model,
		MaxTokens:   p.maxTokens,
		Temperature: p.temperature,
		Messages: []message{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: text},
		},
	}
	return json.Marshal(req)
}

func (p *Provider) doRequest(ctx context.Context, body []byte) (string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", false, fmt.Errorf("crear request HTTP: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		// Error de red o timeout: retryable si el contexto no está cancelado
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return "", false, err
		}
		return "", true, fmt.Errorf("error de red: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", true, fmt.Errorf("leer respuesta: %w", err)
	}

	// 429 Rate limit: retryable
	if resp.StatusCode == http.StatusTooManyRequests {
		return "", true, fmt.Errorf("rate limit (429): %s", string(respBody))
	}

	// 4xx cliente (salvo 429): no retryable
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return "", false, fmt.Errorf("error del proveedor IA (%d): %s", resp.StatusCode, string(respBody))
	}

	// 5xx servidor: no retryable por simplicidad (KISS)
	if resp.StatusCode >= 500 {
		return "", false, fmt.Errorf("error del proveedor IA (%d): %s", resp.StatusCode, string(respBody))
	}

	// 200 OK: parsear respuesta
	var parsed chatCompletionResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return "", false, fmt.Errorf("decodificar respuesta IA: %w", err)
	}

	if len(parsed.Choices) == 0 || len(strings.TrimSpace(parsed.Choices[0].Message.Content)) == 0 {
		return "", false, domain.NewIAProviderError("contenido vacío en respuesta de IA")
	}

	return parsed.Choices[0].Message.Content, false, nil
}