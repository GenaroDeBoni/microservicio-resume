package domain

import (
	"context"
)

// SummarizerProvider es el contrato para generar un resumen a partir de un texto.
// Permite cambiar de proveedor/modelo de IA sin afectar la lógica de negocio (DIP).
type SummarizerProvider interface {
	Summarize(ctx context.Context, text string) (string, error)
	// Model devuelve el identificador del modelo usado, para trazabilidad en los eventos.
	Model() string
}

// SummaryRepository es el contrato para persistir/consultar resúmenes.
// Se implementa vía HTTP contra microservicio-db-go (no acceso directo a MongoDB).
type SummaryRepository interface {
	// Exists verifica si ya existe un resumen para el document_id (idempotencia).
	Exists(ctx context.Context, documentID string) (bool, error)
	// GetTextContent obtiene el texto extraído del documento para poder resumirlo.
	// El texto proviene de microservicio-db-go (que ya lo tiene persistido tras
	// la extracción del microservicio-extractor).
	GetTextContent(ctx context.Context, documentID string) (string, error)
	// Upsert crea o actualiza el resumen asociado al document_id.
	Upsert(ctx context.Context, documentID string, summary *Summary) error
}

// PDFTextExtractor es el contrato para extraer texto plano de las primeras N páginas de un PDF.
type PDFTextExtractor interface {
	ExtractFirstPages(ctx context.Context, data []byte, maxPages int) (text string, pageCount int, err error)
}

// HealthChecker reporta la salud de las dependencias.
type HealthChecker interface {
	Ping(ctx context.Context) error
}