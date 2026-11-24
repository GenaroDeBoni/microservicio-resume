package application

import (
	"context"
	"strings"
	"time"

	"microservicio-resume/domain"
)

// SummarizeRequest es la entrada del caso de uso GenerateSummary.
type SummarizeRequest struct {
	DocumentID string
	Filename   string
}

// SummarizeResponse es la salida exitosa del caso de uso GenerateSummary.
type SummarizeResponse struct {
	DocumentID string
	Summary    string
	Model      string
}

// SummarizerProvider es la seam que el caso de uso usa para generar el resumen.
// Está satisfecha por *infrastructure/ai.Provider.
type SummarizerProvider interface {
	Summarize(ctx context.Context, text string) (string, error)
	Model() string
}

// SummaryRepository es la seam que el caso de uso usa para persistir el resumen.
// Está satisfecha por *infrastructure/http.Client.
type SummaryRepository interface {
	// Exists verifica si ya existe un resumen para el document_id (idempotencia).
	Exists(ctx context.Context, documentID string) (bool, error)
	// GetTextContent obtiene el texto extraído del documento para poder resumirlo.
	GetTextContent(ctx context.Context, documentID string) (string, error)
	// Upsert crea o actualiza el resumen asociado al document_id.
	Upsert(ctx context.Context, documentID string, summary *domain.Summary) error
}

// GenerateSummaryUseCase orquesta el flujo: verificar idempotencia → obtener texto → generar resumen con IA → persistir.
// Depende solo de interfaces del dominio (DIP).
type GenerateSummaryUseCase struct {
	summarizer SummarizerProvider
	repo       SummaryRepository
}

// NewGenerateSummaryUseCase construye el caso de uso con sus dependencias inyectadas.
func NewGenerateSummaryUseCase(summarizer SummarizerProvider, repo SummaryRepository) *GenerateSummaryUseCase {
	return &GenerateSummaryUseCase{
		summarizer: summarizer,
		repo:       repo,
	}
}

// Execute procesa una solicitud de resumen.
// Flujo:
//  1. Verificar idempotencia: si ya existe resumen para document_id, devolver error de dominio.
//  2. Obtener el texto del documento desde db-go.
//  3. Generar resumen con IA sobre el texto obtenido.
//  4. Persistir el resumen vía SummaryRepository.
func (uc *GenerateSummaryUseCase) Execute(ctx context.Context, req SummarizeRequest) (*SummarizeResponse, error) {
	// Paso 1: Idempotencia — no resumir dos veces el mismo documento.
	exists, err := uc.repo.Exists(ctx, req.DocumentID)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, domain.NewSummaryAlreadyExistsError(req.DocumentID)
	}

	// Paso 2: Obtener texto del documento desde microservicio-db-go.
	text, err := uc.repo.GetTextContent(ctx, req.DocumentID)
	if err != nil {
		return nil, err
	}

	// Paso 3: Validar texto de entrada.
	if len(strings.TrimSpace(text)) == 0 {
		return nil, domain.ErrEmptyText
	}

	// Paso 4: Generar resumen con IA.
	summaryText, err := uc.summarizer.Summarize(ctx, text)
	if err != nil {
		return nil, err
	}

	// Paso 5: Persistir resumen.
	now := time.Now().UTC()
	entity := &domain.Summary{
		DocumentID: req.DocumentID,
		Filename:   req.Filename,
		Summary:    summaryText,
		Model:      uc.summarizer.Model(),
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := uc.repo.Upsert(ctx, req.DocumentID, entity); err != nil {
		return nil, err
	}

	return &SummarizeResponse{
		DocumentID: req.DocumentID,
		Summary:    summaryText,
		Model:      entity.Model,
	}, nil
}