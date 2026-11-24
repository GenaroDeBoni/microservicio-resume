package presentation

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// HealthChecker define la interfaz para verificar salud de dependencias.
type HealthChecker interface {
	Health(ctx context.Context) error
}

// Config es la configuración de presentación (solo ErrBaseURL para RFC 9457).
type Config struct {
	ErrBaseURL string
}

// Handler conecta la capa HTTP con el caso de uso de resumen.
type Handler struct {
	cfg      Config
	dbClient HealthChecker
}

// NewHandler construye el handler de presentación.
func NewHandler(cfg Config, dbClient HealthChecker) *Handler {
	return &Handler{cfg: cfg, dbClient: dbClient}
}

// HealthLiveness maneja GET /health/live (liveness probe, solo interno).
func (h *Handler) HealthLiveness(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// HealthReadiness maneja GET /health (readiness probe, solo interno).
// Verifica conectividad con microservicio-db-go.
func (h *Handler) HealthReadiness(c *gin.Context) {
	dbStatus := "ok"
	if h.dbClient != nil {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		if err := h.dbClient.Health(ctx); err != nil {
			dbStatus = "unhealthy: " + err.Error()
		}
	} else {
		dbStatus = "not configured"
	}

	c.JSON(http.StatusOK, gin.H{
		"status":   "ok",
		"service":  "microservicio-resume",
		"database": dbStatus,
	})
}

// NewRouter conecta las rutas HTTP con middleware de recovery.
func NewRouter(cfg Config, dbClient HealthChecker) *gin.Engine {
	r := gin.New()
	r.Use(gin.RecoveryWithWriter(nil, func(c *gin.Context, recovered any) {
		c.Abort()
		writeProblem(c, cfg.ErrBaseURL, "server-error", c.Request.URL.Path)
	}))

	h := NewHandler(cfg, dbClient)

	// Health endpoints (solo internos, no expuestos por Traefik)
	r.GET("/health/live", h.HealthLiveness)
	r.GET("/health", h.HealthReadiness)

	return r
}

// writeProblem emite un error RFC 9457.
func writeProblem(c *gin.Context, baseURL, slug, instance string) {
	c.Header("Content-Type", "application/problem+json")
	c.JSON(http.StatusInternalServerError, map[string]any{
		"type":     baseURL + "/" + slug,
		"title":    "Error interno",
		"status":   http.StatusInternalServerError,
		"detail":   slug,
		"instance": instance,
	})
}