package webhook_producer

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
)

func TestIsRetryableStatus(t *testing.T) {
	cases := map[int]bool{
		0: true, 408: true, 429: true, 500: true, 502: true, 503: true,
		400: false, 401: false, 403: false, 404: true, 413: false, 422: false,
	}
	for code, want := range cases {
		if got := isRetryableStatus(code); got != want {
			t.Errorf("isRetryableStatus(%d) = %v, want %v", code, got, want)
		}
	}
}

func TestSendWebhookWithRetry_Attempts(t *testing.T) {
	cases := []struct {
		status int
		want   int32
	}{
		{200, 1},
		{413, 1}, // corpo grande demais: reenviar nao adianta
		{400, 1},
		{408, 3},
		{429, 3},
		{500, 3},
	}
	for _, c := range cases {
		t.Run(fmt.Sprint(c.status), func(t *testing.T) {
			var hits int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				atomic.AddInt32(&hits, 1)
				w.WriteHeader(c.status)
			}))
			defer srv.Close()

			cfg := &config.Config{LogDirectory: t.TempDir()}
			p := &webhookProducer{loggerWrapper: logger_wrapper.NewLoggerManager(cfg)}
			p.sendWebhookWithRetry(srv.URL, []byte(`{}`), 3, time.Millisecond, "inst")

			if got := atomic.LoadInt32(&hits); got != c.want {
				t.Fatalf("status %d: %d tentativas, esperado %d", c.status, got, c.want)
			}
		})
	}
}

// Falha de rede (sem status) continua com retry.
func TestSendWebhookWithRetry_NetworkErrorRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	cfg := &config.Config{LogDirectory: t.TempDir()}
	p := &webhookProducer{loggerWrapper: logger_wrapper.NewLoggerManager(cfg)}
	start := time.Now()
	p.sendWebhookWithRetry(url, []byte(`{}`), 3, 20*time.Millisecond, "inst")
	if time.Since(start) < 50*time.Millisecond {
		t.Fatal("erro de rede deveria reenviar (3 tentativas com espera)")
	}
}
