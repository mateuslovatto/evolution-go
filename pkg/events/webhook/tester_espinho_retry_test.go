package webhook_producer

// QA adversarial (Espinho) — política de retry do webhook.
// Testes de defeito pulam por padrão (não bloqueiam a imagem no CI);
// rode com ESPINHO_DEFECTS=1 para vê-los falhar.

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
)

func espinhoProducer(t *testing.T) *webhookProducer {
	cfg := &config.Config{LogDirectory: t.TempDir()}
	return &webhookProducer{loggerWrapper: logger_wrapper.NewLoggerManager(cfg)}
}

func espinhoHits(t *testing.T, handler func(n int32, w http.ResponseWriter)) (string, *int32) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handler(atomic.AddInt32(&hits, 1), w)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &hits
}

// Verde: 5xx transitório seguido de 200 para no sucesso (não duplica).
func TestEspinho503DepoisSucessoParaNoSucesso(t *testing.T) {
	url, hits := espinhoHits(t, func(n int32, w http.ResponseWriter) {
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	espinhoProducer(t).sendWebhookWithRetry(url, []byte(`{}`), 5, time.Millisecond, "inst")
	if got := atomic.LoadInt32(hits); got != 3 {
		t.Fatalf("esperado 3 tentativas (503, 503, 200), veio %d", got)
	}
}

// Verde: 429 e 408 continuam reenviando até o máximo.
func TestEspinho429E408ReenviamAteOMaximo(t *testing.T) {
	for _, code := range []int{408, 429} {
		url, hits := espinhoHits(t, func(_ int32, w http.ResponseWriter) { w.WriteHeader(code) })
		espinhoProducer(t).sendWebhookWithRetry(url, []byte(`{}`), 5, time.Millisecond, "inst")
		if got := atomic.LoadInt32(hits); got != 5 {
			t.Fatalf("status %d: %d tentativas, esperado 5", code, got)
		}
	}
}

// Verde: 413 (corpo grande) e 401 (token/instância rejeitados pelo SBX) não reenviam.
func TestEspinho413E401NaoReenviam(t *testing.T) {
	for _, code := range []int{401, 413} {
		url, hits := espinhoHits(t, func(_ int32, w http.ResponseWriter) { w.WriteHeader(code) })
		espinhoProducer(t).sendWebhookWithRetry(url, []byte(`{}`), 5, time.Millisecond, "inst")
		if got := atomic.LoadInt32(hits); got != 1 {
			t.Fatalf("status %d: %d tentativas, esperado 1", code, got)
		}
	}
}

// R1 · MÉDIA: 404 deixou de ter retry. O SBX roda atrás do Traefik (Dokploy); durante a
// troca do container da API o roteador some e o Traefik responde 404 "page not found".
// Antes o webhook era reenviado em 30 s e sobrevivia ao deploy; agora a mensagem é
// descartada na primeira tentativa (o SBX nunca devolve 404 em /webhooks/evolution).
// Corrigido: só 400/401/403/413/422 não reenviam.
func TestEspinho404DeProxyNoDeployDeveReenviar(t *testing.T) {
	url, hits := espinhoHits(t, func(n int32, w http.ResponseWriter) {
		if n == 1 {
			http.Error(w, "404 page not found", http.StatusNotFound) // Traefik sem roteador
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	espinhoProducer(t).sendWebhookWithRetry(url, []byte(`{}`), 5, time.Millisecond, "inst")
	if got := atomic.LoadInt32(hits); got != 2 {
		t.Fatalf("404 transitório do proxy: %d tentativa(s); esperado reenviar e entregar na 2ª", got)
	}
}
