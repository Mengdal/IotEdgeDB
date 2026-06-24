package mcp

import (
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type Handler struct {
	server      *mcp.Server
	httpHandler *mcp.StreamableHTTPHandler
}

func NewHandler(cfg Config) *Handler {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "iedb-mcp",
		Version: "1.0.0",
	}, &mcp.ServerOptions{
		Instructions: "iedb is a high-performance time-series database. Use list_databases to discover databases, list_measurements to see tables, describe_measurement to understand schema, and query to run SQL (DuckDB dialect). Always describe a measurement before querying it.",
	})

	RegisterAll(server, cfg)

	httpHandler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		return server
	}, nil)

	return &Handler{
		server:      server,
		httpHandler: httpHandler,
	}
}

func (h *Handler) RegisterRoutes(app *fiber.App) {
	app.All("/api/v1/mcp", h.handle)
}

func (h *Handler) handle(c *fiber.Ctx) error {
	fastReq := c.Request()

	httpReq := &http.Request{
		Method: string(fastReq.Header.Method()),
		URL:    nil,
		Header: make(http.Header),
		Body:   io.NopCloser(strings.NewReader(string(c.Body()))),
	}

	fastReq.Header.VisitAll(func(key, value []byte) {
		httpReq.Header.Add(string(key), string(value))
	})

	scheme := "http"
	if c.Protocol() == "https" {
		scheme = "https"
	}
	httpReq.URL, _ = url.Parse(scheme + "://" + c.Hostname() + c.OriginalURL())

	w := &responseWriter{
		status:  200,
		headers: make(http.Header),
	}

	h.httpHandler.ServeHTTP(w, httpReq)

	c.Status(w.status)
	for key, values := range w.headers {
		for _, value := range values {
			c.Set(key, value)
		}
	}
	return c.Send(w.body)
}

type responseWriter struct {
	status  int
	headers http.Header
	body    []byte
}

func (w *responseWriter) Header() http.Header {
	return w.headers
}

func (w *responseWriter) Write(b []byte) (int, error) {
	w.body = append(w.body, b...)
	return len(b), nil
}

func (w *responseWriter) WriteHeader(statusCode int) {
	w.status = statusCode
}
