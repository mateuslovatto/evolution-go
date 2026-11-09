package whatsmeow_service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/patrickmn/go-cache"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types/events"
	_ "modernc.org/sqlite"
)

// QA adversarial (Urtiga) do fix1: o WhatsApp deslogou o aparelho. O ciclo de
// reconexao pelo mesmo proxy PRECISA parar; senao o numero fica tentando
// conectar depois de removido/banido, que e sinal ruim para a conta.
//
// Estes testes simulam os eventos e exigem CancelProxyRetry. Eles falham no
// HEAD atual e documentam o defeito; nao tocam no WhatsApp.

type cancelSpy struct {
	WhatsmeowService
	starts  int
	cancels int
}

func (s *cancelSpy) StartProxyRetry(string)  { s.starts++ }
func (s *cancelSpy) CancelProxyRetry(string) { s.cancels++ }

func newEventClient(t *testing.T, spy *cancelSpy, repo *repoSpy) *MyClient {
	t.Helper()
	real := &whatsmeowService{
		instanceRepository: repo,
		config:             &config.Config{LogDirectory: os.TempDir()},
		clientPointer:      map[string]*whatsmeow.Client{},
		myClientPointer:    map[string]*MyClient{},
		killChannel:        map[string]chan bool{},
		userInfoCache:      cache.New(cache.NoExpiration, 0),
		loggerWrapper:      logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: os.TempDir()}),
		webhookProducer:    &producerSpy{ch: make(chan []byte, 8)},
	}
	spy.WhatsmeowService = real
	instance := &instance_model.Instance{
		Id:     "inst-logout-qa",
		Token:  "token-logout-qa",
		Name:   "Logout QA",
		Events: "ALL",
	}
	return &MyClient{
		service:            spy,
		Instance:           instance,
		userID:             instance.Id,
		token:              instance.Token,
		subscriptions:      []string{"ALL"},
		instanceRepository: repo,
		loggerWrapper:      real.loggerWrapper,
		userInfoCache:      real.userInfoCache,
		config:             &config.Config{},
		killChannel:        map[string]chan bool{instance.Id: make(chan bool, 1)},
		isProxy:            true,
	}
}

func TestUrtigaLoggedOutStopsProxyRetry(t *testing.T) {
	repo := &repoSpy{}
	spy := &cancelSpy{}
	mycli := newEventClient(t, spy, repo)

	mycli.myEventHandler(&events.LoggedOut{})

	if spy.cancels != 1 {
		t.Fatalf("LoggedOut nao cancelou o ciclo de retry: cancels=%d, quer 1", spy.cancels)
	}
}

func TestUrtigaTemporaryBanStopsProxyRetry(t *testing.T) {
	repo := &repoSpy{}
	spy := &cancelSpy{}
	mycli := newEventClient(t, spy, repo)

	mycli.myEventHandler(&events.TemporaryBan{})

	if spy.cancels != 1 {
		t.Fatalf("TemporaryBan nao cancelou o ciclo de retry: cancels=%d, quer 1", spy.cancels)
	}
}

func TestUrtigaStreamReplacedStopsProxyRetry(t *testing.T) {
	repo := &repoSpy{}
	spy := &cancelSpy{}
	mycli := newEventClient(t, spy, repo)

	mycli.myEventHandler(&events.StreamReplaced{})

	if spy.cancels != 1 {
		t.Fatalf("StreamReplaced nao cancelou o ciclo de retry: cancels=%d, quer 1", spy.cancels)
	}
}

func newSQLiteService(t *testing.T, repo *repoSpy, cfg *config.Config, backoff []time.Duration) *whatsmeowService {
	t.Helper()
	container, err := sqlstore.New(
		context.Background(),
		"sqlite",
		"file:"+filepath.Join(t.TempDir(), "qa.sqlite")+"?_pragma=foreign_keys(1)",
		nil,
	)
	if err != nil {
		t.Fatalf("sqlstore: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })
	return &whatsmeowService{
		instanceRepository: repo,
		config:             cfg,
		clientPointer:      map[string]*whatsmeow.Client{},
		myClientPointer:    map[string]*MyClient{},
		killChannel:        map[string]chan bool{},
		userInfoCache:      cache.New(cache.NoExpiration, 0),
		loggerWrapper:      logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: os.TempDir()}),
		storeContainer:     &storeContainerHolder{container: container},
		proxyRetryBackoff:  backoff,
	}
}

// O ciclo de retry continua tentando depois do LoggedOut: o loop so sai com
// cancel, Jid vazio ou Proxy vazio. O handler nao chama CancelProxyRetry.
func TestUrtigaLoggedOutKeepsRetrying(t *testing.T) {
	repo := &repoSpy{}
	repo.instance = &instance_model.Instance{
		Id:     "inst-logout-loop",
		Token:  "token-logout-loop",
		Name:   "Logout Loop QA",
		Jid:    "5511999999999@s.whatsapp.net",
		Events: "ALL",
		Proxy:  `{"protocol":"socks5","host":"127.0.0.1","port":"1080"}`,
	}
	service := newSQLiteService(t, repo, &config.Config{
		LogDirectory:         os.TempDir(),
		WhatsappVersionMajor: 2,
		WhatsappVersionMinor: 3000,
		WhatsappVersionPatch: 1,
	}, []time.Duration{100 * time.Millisecond})

	var attempts int32
	service.connector = func(*ClientData, *whatsmeow.Client, bool) error {
		atomic.AddInt32(&attempts, 1)
		return errors.New("proxy down")
	}

	service.StartProxyRetry(repo.instance.Id)
	deadline := time.Now().Add(3 * time.Second)
	for atomic.LoadInt32(&attempts) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if atomic.LoadInt32(&attempts) == 0 {
		t.Fatal("o ciclo nem comecou")
	}

	mycli := &MyClient{
		service:            service,
		Instance:           repo.instance,
		userID:             repo.instance.Id,
		token:              repo.instance.Token,
		instanceRepository: repo,
		loggerWrapper:      service.loggerWrapper,
		userInfoCache:      service.userInfoCache,
		config:             &config.Config{},
		killChannel:        map[string]chan bool{repo.instance.Id: make(chan bool, 1)},
		isProxy:            true,
	}
	mycli.myEventHandler(&events.LoggedOut{})

	before := atomic.LoadInt32(&attempts)
	time.Sleep(700 * time.Millisecond)
	after := atomic.LoadInt32(&attempts)
	service.CancelProxyRetry(repo.instance.Id)

	if after > before {
		t.Fatalf("LoggedOut nao parou o ciclo: tentativas %d -> %d", before, after)
	}
}

// Start/Cancel normal nao pode deixar goroutine vazando.
func TestUrtigaProxyRetryNoGoroutineLeak(t *testing.T) {
	repo := &repoSpy{}
	repo.instance = &instance_model.Instance{
		Id:    "inst-goroutine",
		Jid:   "5511999999999@s.whatsapp.net",
		Proxy: `{"protocol":"socks5","host":"127.0.0.1","port":"1080"}`,
	}
	service := newSQLiteService(t, repo, &config.Config{LogDirectory: os.TempDir()}, []time.Duration{time.Hour})

	before := runtime.NumGoroutine()
	service.StartProxyRetry(repo.instance.Id)
	service.CancelProxyRetry(repo.instance.Id)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		state := service.proxyRegistry()
		state.mu.Lock()
		pending := len(state.cycles)
		state.mu.Unlock()
		if pending == 0 && runtime.NumGoroutine() <= before+1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutines antes=%d depois=%d", before, runtime.NumGoroutine())
}

// Proxy global configurado + instancia com Proxy "null" (criada sem proxy):
// startInstance nao aplica o proxy global e o start sai direto. F1 exige que
// proxy configurado nunca caia para a conexao direta.
func TestUrtigaGlobalProxyNotBypassedByNullInstanceProxy(t *testing.T) {
	repo := &repoSpy{}
	repo.instance = &instance_model.Instance{
		Id:     "inst-null-proxy",
		Token:  "token-null-proxy",
		Name:   "Null Proxy QA",
		Jid:    "5511999999999@s.whatsapp.net",
		Events: "ALL",
		Proxy:  "null",
	}
	cfg := &config.Config{
		LogDirectory:         os.TempDir(),
		ProxyHost:            "127.0.0.1",
		ProxyPort:            "1080",
		ProxyUsername:        "u",
		ProxyPassword:        "p",
		WhatsappVersionMajor: 2,
		WhatsappVersionMinor: 3000,
		WhatsappVersionPatch: 1,
	}
	service := newSQLiteService(t, repo, cfg, nil)
	seen := make(chan bool, 1)
	service.connector = func(_ *ClientData, _ *whatsmeow.Client, proxyConfigured bool) error {
		seen <- proxyConfigured
		return errors.New("fake dial")
	}

	if err := service.startInstance(repo.instance.Id, nil); err != nil {
		t.Fatalf("startInstance: %v", err)
	}
	select {
	case configured := <-seen:
		if !configured {
			t.Fatal("proxy global configurado, mas o start saiu direto (proxyConfigured=false)")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("start nao chegou ao connector")
	}
}

// StartClient tem receiver por valor e entrega `&w` (uma copia) ao MyClient.
// O registry do retry fica na copia; CancelProxyRetry chamado pelo service
// original (SetProxy/RemoveProxy/Logout/Delete) nao ve o ciclo da copia.
func TestUrtigaRetryRegistryCopiedByValue(t *testing.T) {
	repo := &repoSpy{}
	repo.instance = &instance_model.Instance{
		Id:    "inst-copy-registry",
		Jid:   "5511999999999@s.whatsapp.net",
		Proxy: `{"protocol":"socks5","host":"127.0.0.1","port":"1080"}`,
	}
	original := newSQLiteService(t, repo, &config.Config{LogDirectory: os.TempDir()}, []time.Duration{time.Hour})
	copied := *original // mesma copia que StartClient faz no receiver por valor

	copied.StartProxyRetry(repo.instance.Id)
	original.CancelProxyRetry(repo.instance.Id) // rota da API

	state := copied.proxyRegistry()
	state.mu.Lock()
	pending := len(state.cycles)
	state.mu.Unlock()
	copied.CancelProxyRetry(repo.instance.Id)
	if pending != 0 {
		t.Fatalf("CancelProxyRetry no service original nao parou o ciclo iniciado pela copia do StartClient (%d aberto)", pending)
	}
}
