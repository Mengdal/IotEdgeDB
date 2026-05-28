package api

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"iedb/internal/auth"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// ProxyHandler proxies external HTTP GET requests to bypass browser CORS restrictions.
type ProxyHandler struct {
	client *http.Client
	logger zerolog.Logger

	authManager *auth.AuthManager
}

// NewProxyHandler creates a new ProxyHandler.
func NewProxyHandler(logger zerolog.Logger) *ProxyHandler {
	return &ProxyHandler{
		client: &http.Client{Timeout: 30 * time.Second},
		logger: logger.With().Str("component", "proxy-handler").Logger(),
	}
}

// SetAuthManager sets the auth manager for write-level authentication.
func (h *ProxyHandler) SetAuthManager(am *auth.AuthManager) {
	h.authManager = am
}

// RegisterRoutes registers the proxy route.
func (h *ProxyHandler) RegisterRoutes(app *fiber.App) {
	writeAuth := withWriteAuth(h.authManager)
	app.Get("/api/v1/proxy/fetch", writeAuth, h.handleFetch)
	h.logger.Info().Msg("Proxy routes registered")
}

// handleFetch fetches content from a user-supplied URL and streams it back.
func (h *ProxyHandler) handleFetch(c *fiber.Ctx) error {
	rawURL := c.Query("url")
	if rawURL == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "missing 'url' query parameter",
		})
	}

	parsed, err := url.Parse(rawURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid URL: only http/https schemes are allowed",
		})
	}

	if parsed.Host == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid URL: missing host",
		})
	}

	req, err := http.NewRequestWithContext(c.Context(), http.MethodGet, rawURL, nil)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("failed to create request: %v", err),
		})
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{
			"error": fmt.Sprintf("failed to fetch URL: %v", err),
		})
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{
			"error": fmt.Sprintf("failed to read response body: %v", err),
		})
	}

	c.Set("Content-Type", resp.Header.Get("Content-Type"))
	c.Set("X-Proxy-Status", fmt.Sprintf("%d", resp.StatusCode))

	return c.Send(body)
}
