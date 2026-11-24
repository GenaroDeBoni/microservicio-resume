package http

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

// Client implementa domain.SummaryRepository mediante HTTP contra
// microservicio-db-go (sin acceso directo a MongoDB).
type Client struct {
	baseURL         string
	timeout         time.Duration
	client          *http.Client
	apiKey          string // opcional: autenticación Bearer contra db-go
	waitForDocument bool
	waitTimeout     time.Duration
	waitInterval    time.Duration
}

// NewClient construye un cliente HTTP para el microservicio de persistencia.
// Sin espera: conserva el comportamiento usado en tests y en consumos donde
// el documento ya existe.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: baseURL,
		timeout: timeout,
		client:  &http.Client{Timeout: timeout},
	}
}

// NewClientWithWait habilita la espera acotada de documentos. Es necesaria en
// producción porque Monolitico publica pdf.validated antes de persistir.
// apiKey es opcional; si se envía, se añade como header Authorization: Bearer.
func NewClientWithWait(baseURL string, timeout, waitTimeout, waitInterval time.Duration, apiKey ...string) *Client {
	if waitTimeout <= 0 {
		waitTimeout = 2 * time.Minute
	}
	if waitInterval <= 0 {
		waitInterval = time.Second
	}
	c := NewClient(baseURL, timeout)
	c.waitForDocument = true
	c.waitTimeout = waitTimeout
	c.waitInterval = waitInterval
	if len(apiKey) > 0 {
		c.apiKey = apiKey[0]
	}
	return c
}

// setAuthHeader añade el header Authorization: Bearer si hay apiKey configurado.
func (c *Client) setAuthHeader(req *http.Request) {
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
}

// getDocument obtiene el documento por su identificador público (ObjectID o
// checksum SHA-256). Si está habilitada la espera, reintenta ante 404 hasta
// que el documento aparece o se agota DB_WAIT_TIMEOUT.
func (c *Client) getDocument(ctx context.Context, documentID string) (map[string]any, bool, error) {
	deadline := time.Now().Add(c.waitTimeout)

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/documents/"+documentID, nil)
		if err != nil {
			return nil, false, fmt.Errorf("crear request Exists: %w", err)
		}
		c.setAuthHeader(req)

		resp, err := c.client.Do(req)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return nil, false, err
			}
			return nil, false, fmt.Errorf("error de red Exists: %w", err)
		}

		if resp.StatusCode == http.StatusOK {
			defer func() { _ = resp.Body.Close() }()
			var body map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				return nil, false, fmt.Errorf("decodificar documento: %w", err)
			}
			return body, true, nil
		}

		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			return nil, false, fmt.Errorf("respuesta inesperada (%d) al buscar documento", resp.StatusCode)
		}
		if !c.waitForDocument {
			return nil, false, nil
		}
		if time.Now().After(deadline) {
			return nil, false, fmt.Errorf("documento %s no disponible tras %s", documentID, c.waitTimeout)
		}

		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-time.After(c.waitInterval):
		}
	}
}

// Exists verifica si ya existe un resumen para el document_id.
// El documento puede existir sin resumen; por eso se inspecciona el campo
// "summary" y no la existencia del documento en sí.
func (c *Client) Exists(ctx context.Context, documentID string) (bool, error) {
	doc, ok, err := c.getDocument(ctx, documentID)
	if err != nil || !ok {
		return false, err
	}

	summary, _ := doc["summary"].(string)
	return strings.TrimSpace(summary) != "", nil
}

// GetTextContent obtiene el texto extraído del documento (campo "text_content")
// desde microservicio-db-go. Reintenta ante 404 hasta que el documento exista
// o se agota DB_WAIT_TIMEOUT, porque el evento se publica antes de persistir.
func (c *Client) GetTextContent(ctx context.Context, documentID string) (string, error) {
	doc, ok, err := c.getDocument(ctx, documentID)
	if err != nil || !ok {
		return "", err
	}

	text, _ := doc["text_content"].(string)
	return strings.TrimSpace(text), nil
}

// Upsert crea o actualiza el resumen asociado al document_id.
// Llama a PATCH /api/v1/documents/{id}/summary con body {"summary": "..."}.
func (c *Client) Upsert(ctx context.Context, documentID string, summary *domain.Summary) error {
	payload, err := json.Marshal(map[string]any{
		"summary": summary.Summary,
	})
	if err != nil {
		return fmt.Errorf("marcar payload Upsert: %w", err)
	}

	// El evento se publica antes de persistir: esperamos a que el documento
	// exista para no perder el resumen en un 404 transitorio.
	if c.waitForDocument {
		_, ok, err := c.getDocument(ctx, documentID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("documento %s no disponible para persistir resumen", documentID)
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, c.baseURL+"/api/v1/documents/"+documentID+"/summary", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("crear request Upsert: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.setAuthHeader(req)

	resp, err := c.client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return err
		}
		return fmt.Errorf("error de red Upsert: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusCreated:
		return nil
	case http.StatusConflict:
		return domain.NewSummaryAlreadyExistsError(documentID)
	case http.StatusNotFound:
		return domain.NewSummaryNotFoundError(documentID)
	default:
		return fmt.Errorf("error al persistir resumen (%d): %s", resp.StatusCode, string(body))
	}
}