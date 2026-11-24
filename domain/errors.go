package domain

import (
	"errors"
	"time"
)

// Summary representa el resumen generado de un documento, asociado a su ID original.
type Summary struct {
	DocumentID   string    `json:"document_id" bson:"document_id"`
	Filename     string    `json:"filename" bson:"filename"`
	Summary      string    `json:"summary" bson:"summary"`
	Model        string    `json:"model" bson:"model"`
	CreatedAt    time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt    time.Time `json:"updated_at" bson:"updated_at"`
}

// DocumentUpdateSummary representa los campos para actualizar únicamente el resumen (PATCH semántico).
type DocumentUpdateSummary struct {
	Summary *string `json:"summary,omitempty" bson:"summary,omitempty"`
}

// GetChanges devuelve solo los campos provistos por el cliente.
func (u *DocumentUpdateSummary) GetChanges() map[string]any {
	changes := make(map[string]any)
	if u.Summary != nil {
		changes["summary"] = *u.Summary
	}
	return changes
}

// DomainError es la base de todos los errores de dominio.
type DomainError struct {
	Code   string
	Detail string
}

func (e *DomainError) Error() string {
	return e.Detail
}

// Error codes estables para mapeo a RFC 9457.
const (
	ErrorCodeSummaryNotFound       = "SUMMARY_NOT_FOUND"
	ErrorCodeInvalidDocumentID     = "INVALID_DOCUMENT_ID"
	ErrorCodeSummaryAlreadyExists  = "SUMMARY_ALREADY_EXISTS"
	ErrorCodeIAProviderError       = "IA_PROVIDER_ERROR"
	ErrorCodeInternal              = "INTERNAL_ERROR"
)

var (
	// ErrSummaryNotFound indica que no se encontró un resumen para el documento.
	ErrSummaryNotFound = errors.New("resumen no encontrado")
	// ErrInvalidDocumentID indica que el ID de documento no tiene formato válido.
	ErrInvalidDocumentID = errors.New("ID de documento inválido")
	// ErrSummaryAlreadyExists indica que el documento ya tiene un resumen (idempotencia).
	ErrSummaryAlreadyExists = errors.New("resumen ya existe para este documento")
	// ErrEmptyText indica que el texto a resumir está vacío.
	ErrEmptyText = errors.New("texto vacío")
)

// SummaryNotFoundError envuelve el error de dominio para resumen no encontrado.
type SummaryNotFoundError struct {
	DomainError
}

func NewSummaryNotFoundError(docID string) *SummaryNotFoundError {
	return &SummaryNotFoundError{
		DomainError: DomainError{
			Code:   ErrorCodeSummaryNotFound,
			Detail: "Resumen para el documento '" + docID + "' no encontrado.",
		},
	}
}

func (e *SummaryNotFoundError) Is(target error) bool {
	return target == ErrSummaryNotFound
}

// InvalidDocumentIdError indica que el ID no tiene formato válido.
type InvalidDocumentIdError struct {
	DomainError
}

func NewInvalidDocumentIdError(docID string) *InvalidDocumentIdError {
	return &InvalidDocumentIdError{
		DomainError: DomainError{
			Code:   ErrorCodeInvalidDocumentID,
			Detail: "'" + docID + "' no es un ID válido.",
		},
	}
}

func (e *InvalidDocumentIdError) Is(target error) bool {
	return target == ErrInvalidDocumentID
}

// SummaryAlreadyExistsError indica que el resumen ya existe (idempotencia).
type SummaryAlreadyExistsError struct {
	DomainError
}

func NewSummaryAlreadyExistsError(docID string) *SummaryAlreadyExistsError {
	return &SummaryAlreadyExistsError{
		DomainError: DomainError{
			Code:   ErrorCodeSummaryAlreadyExists,
			Detail: "El documento '" + docID + "' ya tiene un resumen generado.",
		},
	}
}

func (e *SummaryAlreadyExistsError) Is(target error) bool {
	return target == ErrSummaryAlreadyExists
}

// IAProviderError indica un fallo en el proveedor de IA tras agotar reintentos.
type IAProviderError struct {
	DomainError
}

func NewIAProviderError(detail string) *IAProviderError {
	return &IAProviderError{
		DomainError: DomainError{
			Code:   ErrorCodeIAProviderError,
			Detail: detail,
		},
	}
}

func (e *IAProviderError) Is(target error) bool {
	return target == ErrEmptyText // no-op; se usa tipo concreto
}

// InternalError para errores inesperados.
var ErrInternal = errors.New("error interno")

type InternalError struct {
	DomainError
}

func NewInternalError(detail string) *InternalError {
	return &InternalError{
		DomainError: DomainError{
			Code:   ErrorCodeInternal,
			Detail: detail,
		},
	}
}

func (e *InternalError) Is(target error) bool {
	return target == ErrInternal
}

// SlugFor mapea un error de dominio a su slug RFC 9457 estable.
func SlugFor(err error) string {
	switch {
	case errors.Is(err, ErrSummaryNotFound):
		return "summary-not-found"
	case errors.Is(err, ErrInvalidDocumentID):
		return "invalid-document-id"
	case errors.Is(err, ErrSummaryAlreadyExists):
		return "summary-already-exists"
	case errors.Is(err, ErrEmptyText):
		return "empty-text"
	default:
		return "server-error"
	}
}