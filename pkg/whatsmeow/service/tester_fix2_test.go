package whatsmeow_service

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/patrickmn/go-cache"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types/events"
)

// Fix2 (Urtiga): helper único decide se há proxy; "" / "null" / Host vazio
// caem no global quando ele existe.
func TestFix2HasConfiguredProxy(t *testing.T) {
	global := []string{"global.example", "1080", "u", "p"}
	cases := []struct {
		name       string
		instance   string
		wantGlobal bool
		wantAny    bool
	}{
		{name: "vazio usa global", instance: "", wantGlobal: true, wantAny: true},
		{name: "null usa global", instance: "null", wantGlobal: true, wantAny: true},
		{name: "host vazio usa global", instance: `{"protocol":"socks5","host":"","port":"1080"}`, wantGlobal: true, wantAny: true},
		{name: "proxy da instância vence", instance: `{"protocol":"socks5","host":"phone.example","port":"1080"}`, wantAny: true},
		{name: "sem nada não tem proxy", instance: "null", wantAny: false},
		{name: "json inválido é proxy configurado (falha fechada)", instance: `{not-json`, wantAny: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			host, port, user, pass := "", "", "", ""
			if tc.wantGlobal {
				host, port, user, pass = global[0], global[1], global[2], global[3]
			}
			got := HasConfiguredProxy(tc.instance, host, port, user, pass)
			if got != tc.wantAny {
				t.Fatalf("HasConfiguredProxy(%q) = %v, quer %v", tc.instance, got, tc.wantAny)
			}
		})
	}

	if HasConfiguredProxy("", "", "", "", "") {
		t.Fatal("sem proxy da instância e sem global não deve ter proxy")
	}
}

// C1: ConnectFailure de sessão/ban/cliente desatualizado cancela o ciclo; os de
// rede continuam no ciclo.
func TestFix2TerminalConnectFailureCancelsProxyRetry(t *testing.T) {
	terminal := []events.ConnectFailureReason{
		events.ConnectFailureLoggedOut,
		events.ConnectFailureTempBanned,
		events.ConnectFailureMainDeviceGone,
		events.ConnectFailureUnknownLogout,
		events.ConnectFailureClientOutdated,
		events.ConnectFailureBadUserAgent,
		events.ConnectFailureCATExpired,
		events.ConnectFailureCATInvalid,
	}
	for _, reason := range terminal {
		t.Run(reason.String(), func(t *testing.T) {
			spy := &cancelSpy{}
			mycli := newEventClient(t, spy, &repoSpy{})
			mycli.myEventHandler(&events.ConnectFailure{Reason: reason})
			if spy.cancels != 1 {
				t.Fatalf("reason %s não cancelou: cancels=%d", reason, spy.cancels)
			}
		})
	}

	for _, reason := range []events.ConnectFailureReason{
		events.ConnectFailureGeneric,
		events.ConnectFailureInternalServerError,
		events.ConnectFailureServiceUnavailable,
	} {
		t.Run("rede "+reason.String(), func(t *testing.T) {
			spy := &cancelSpy{}
			mycli := newEventClient(t, spy, &repoSpy{})
			mycli.myEventHandler(&events.ConnectFailure{Reason: reason})
			if spy.cancels != 0 {
				t.Fatalf("reason de rede %s cancelou o ciclo", reason)
			}
		})
	}
}

// Obs1: o primeiro EOF no start emite o ConnectFailure na hora; o retry interno
// de 5 s continua (aqui reduzido para o teste).
func TestFix2EOFEmitsConnectFailureBeforeRetry(t *testing.T) {
	producer := &producerSpy{ch: make(chan []byte, 4)}
	service := whatsmeowService{
		instanceRepository: &repoSpy{},
		config:             &config.Config{LogDirectory: os.TempDir()},
		clientPointer:      map[string]*whatsmeow.Client{},
		myClientPointer:    map[string]*MyClient{},
		killChannel:        map[string](chan bool){},
		userInfoCache:      cache.New(cache.NoExpiration, 0),
		loggerWrapper:      logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: os.TempDir()}),
		webhookProducer:    producer,
		proxyEOFRetryDelay: 10 * time.Millisecond,
	}
	instance := &instance_model.Instance{Id: "inst-eof", Token: "tok-eof", Name: "EOF", Webhook: "http://127.0.0.1:1/hook", Events: "ALL"}
	cd := &ClientData{Instance: instance, IsProxy: true}

	started := time.Now()
	calls := 0
	err := service.connectStrictWith(cd, nil, true, func() error {
		calls++
		if calls == 1 {
			return errors.New("websocket: close 1006: EOF")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("connectStrict: %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatalf("retry interno demorou demais no teste: %v", time.Since(started))
	}
	select {
	case payload := <-producer.ch:
		text := string(payload)
		if !strings.Contains(text, `"ConnectFailure"`) ||
			!strings.Contains(text, `"reason":"proxy_error:unreachable"`) {
			t.Fatalf("evento inesperado: %s", text)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("ConnectFailure do primeiro EOF não foi emitido antes do retry")
	}
}

// Obs3: proxy inválido no cliente de mídia falha fechado, nunca cai no direto.
func TestFix2MediaClientFailsClosed(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := mediaHTTPClient("not-a-supported-proxy-url")
	resp, err := client.Get(server.URL)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("cliente de mídia com proxy inválido caiu no direto")
	}
	if atomic.LoadInt32(&hits) != 0 {
		t.Fatal("requisição direta chegou ao destino com proxy inválido")
	}
}
