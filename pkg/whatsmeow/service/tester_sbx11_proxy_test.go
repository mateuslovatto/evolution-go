package whatsmeow_service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	"github.com/evolution-foundation/evolution-go/pkg/events/interfaces"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	instance_repository "github.com/evolution-foundation/evolution-go/pkg/instance/repository"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/patrickmn/go-cache"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types/events"
	_ "modernc.org/sqlite"
)

// fakeSocks5AuthFail is a local SOCKS5 server that always rejects the
// username/password step and counts CONNECT commands (which must stay zero).
func fakeSocks5AuthFail(t *testing.T) (addr string, accepts *int32, connects *int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	accepts = new(int32)
	connects = new(int32)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			atomic.AddInt32(accepts, 1)
			go func(c net.Conn) {
				defer c.Close()
				head := make([]byte, 2)
				if _, err := io.ReadFull(c, head); err != nil || head[0] != 0x05 {
					return
				}
				methods := make([]byte, int(head[1]))
				if _, err := io.ReadFull(c, methods); err != nil {
					return
				}
				// Only user/pass auth (0x02).
				if _, err := c.Write([]byte{0x05, 0x02}); err != nil {
					return
				}
				auth := make([]byte, 2)
				if _, err := io.ReadFull(c, auth); err != nil {
					return
				}
				user := make([]byte, int(auth[1]))
				if _, err := io.ReadFull(c, user); err != nil {
					return
				}
				plen := make([]byte, 1)
				if _, err := io.ReadFull(c, plen); err != nil {
					return
				}
				if _, err := io.ReadFull(c, make([]byte, int(plen[0]))); err != nil {
					return
				}
				// Auth failure. No CONNECT command is ever read.
				_, _ = c.Write([]byte{0x01, 0x01})
			}(conn)
		}
	}()
	return ln.Addr().String(), accepts, connects
}

type repoSpy struct {
	instance_repository.InstanceRepository
	instance  *instance_model.Instance
	connected bool
	reason    string
	calls     int
}

func (r *repoSpy) GetInstanceByID(string) (*instance_model.Instance, error) {
	if r.instance == nil {
		return nil, nil
	}
	return r.instance, nil
}

func (r *repoSpy) UpdateConnected(_ string, status bool, reason string) error {
	r.calls++
	r.connected = status
	r.reason = reason
	return nil
}

type producerSpy struct {
	ch chan []byte
}

func (p *producerSpy) Produce(_ string, payload []byte, _ string, _ string) error {
	select {
	case p.ch <- append([]byte(nil), payload...):
	default:
	}
	return nil
}

func (p *producerSpy) CreateGlobalQueues() error { return nil }

var _ producer_interfaces.Producer = (*producerSpy)(nil)

type reconnectSpy struct {
	WhatsmeowService
	calls   int
	retries int
}

func (s *reconnectSpy) ReconnectClient(string) error {
	s.calls++
	return nil
}

func (s *reconnectSpy) StartProxyRetry(string) { s.retries++ }

func (s *reconnectSpy) CancelProxyRetry(string) {}

func newTestService(repo *repoSpy, producer *producerSpy) whatsmeowService {
	return whatsmeowService{
		instanceRepository: repo,
		config:             &config.Config{LogDirectory: os.TempDir()},
		clientPointer:      map[string]*whatsmeow.Client{},
		myClientPointer:    map[string]*MyClient{},
		killChannel:        map[string](chan bool){},
		userInfoCache:      cache.New(cache.NoExpiration, 0),
		loggerWrapper:      logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: os.TempDir()}),
		webhookProducer:    producer,
	}
}

func TestProxyFailureReason(t *testing.T) {
	cases := map[string]string{
		"username/password authentication failed":  "auth",
		"socks connect tcp: 407 proxy auth":        "auth",
		"tls: failed to verify certificate: x509":  "tls",
		"socks connect tcp: unknown protocol":      "protocol",
		"dial tcp 10.0.0.1:1080: connect: refused": "unreachable",
	}
	for input, want := range cases {
		if got := proxyFailureReason(errString(input)); got != want {
			t.Errorf("proxyFailureReason(%q) = %q, quer %q", input, got, want)
		}
	}
}

func TestStrictProxyStartFailsClosed(t *testing.T) {
	addr, accepts, connects := fakeSocks5AuthFail(t)

	client := whatsmeow.NewClient(&store.Device{}, nil)
	if err := client.SetProxyAddress("socks5://user:pass@" + addr); err != nil {
		t.Fatalf("SetProxyAddress: %v", err)
	}

	repo := &repoSpy{}
	producer := &producerSpy{ch: make(chan []byte, 1)}
	service := newTestService(repo, producer)
	instance := &instance_model.Instance{
		Id:      "inst-proxy",
		Token:   "token-proxy",
		Name:    "Proxy QA",
		Webhook: "http://127.0.0.1:1/hook",
		Events:  "ALL",
	}
	service.clientPointer[instance.Id] = client
	service.myClientPointer[instance.Id] = &MyClient{}
	cd := &ClientData{Instance: instance, IsProxy: true}

	if err := service.connectStrict(cd, client, true); err == nil {
		t.Fatal("connectStrict devolveu conectado; proxy recusou a autenticação")
	}

	if got := atomic.LoadInt32(accepts); got == 0 {
		t.Fatal("o cliente não passou pelo proxy falso")
	}
	if got := atomic.LoadInt32(connects); got != 0 {
		t.Fatalf("o cliente tentou CONNECT %d vez(es) sem autenticar", got)
	}
	if repo.connected {
		t.Fatal("instância ficou conectada")
	}
	if repo.reason != "proxy_error:auth" {
		t.Fatalf("reason = %q, quer proxy_error:auth", repo.reason)
	}
	if _, ok := service.clientPointer[instance.Id]; ok {
		t.Fatal("clientPointer deveria ter sido limpo no fail closed")
	}

	select {
	case payload := <-producer.ch:
		text := string(payload)
		if !strings.Contains(text, `"ConnectFailure"`) ||
			!strings.Contains(text, `"reason":"proxy_error:auth"`) {
			t.Fatalf("evento inesperado: %s", text)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ConnectFailure não foi emitido")
	}
}

func TestStartClientStrictProxyFailsClosed(t *testing.T) {
	addr, accepts, connects := fakeSocks5AuthFail(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split: %v", err)
	}

	container, err := sqlstore.New(
		context.Background(),
		"sqlite",
		"file:"+filepath.Join(t.TempDir(), "db.sqlite")+"?_pragma=foreign_keys(1)",
		nil,
	)
	if err != nil {
		t.Fatalf("sqlstore: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })

	proxyJSON, _ := json.Marshal(ProxyConfig{
		Protocol: "socks5",
		Host:     host,
		Port:     port,
		Username: "user",
		Password: "pass",
	})
	repo := &repoSpy{}
	producer := &producerSpy{ch: make(chan []byte, 2)}
	service := whatsmeowService{
		instanceRepository: repo,
		config: &config.Config{
			LogDirectory:         os.TempDir(),
			WhatsappVersionMajor: 2,
			WhatsappVersionMinor: 3000,
			WhatsappVersionPatch: 1,
		},
		clientPointer:   map[string]*whatsmeow.Client{},
		myClientPointer: map[string]*MyClient{},
		killChannel:     map[string](chan bool){},
		userInfoCache:   cache.New(cache.NoExpiration, 0),
		loggerWrapper:   logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: os.TempDir()}),
		webhookProducer: producer,
		storeContainer:  &storeContainerHolder{container: container},
	}
	instance := &instance_model.Instance{
		Id:      "inst-start",
		Token:   "token-start",
		Name:    "Start QA",
		Webhook: "http://127.0.0.1:1/hook",
		Events:  "ALL",
		Proxy:   string(proxyJSON),
	}
	cd := &ClientData{Instance: instance, IsProxy: true, Subscriptions: []string{"ALL"}}

	service.StartClient(cd)

	if repo.reason != "proxy_error:auth" {
		t.Fatalf("reason = %q, quer proxy_error:auth", repo.reason)
	}
	if repo.connected {
		t.Fatal("instância ficou conectada")
	}
	if atomic.LoadInt32(accepts) == 0 {
		t.Fatal("StartClient não passou pelo proxy falso")
	}
	if atomic.LoadInt32(connects) != 0 {
		t.Fatalf("StartClient tentou CONNECT %d vez(es) sem autenticar", connects)
	}
	select {
	case payload := <-producer.ch:
		if !strings.Contains(string(payload), `"ConnectFailure"`) ||
			!strings.Contains(string(payload), `"reason":"proxy_error:auth"`) {
			t.Fatalf("evento inesperado: %s", payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ConnectFailure não foi emitido")
	}
}

func TestMidSessionProxyDropEmitsFast(t *testing.T) {
	repo := &repoSpy{}
	producer := &producerSpy{ch: make(chan []byte, 1)}
	service := newTestService(repo, producer)
	restarts := &reconnectSpy{WhatsmeowService: &service}
	instance := &instance_model.Instance{
		Id:      "inst-mid",
		Token:   "token-mid",
		Name:    "Mid QA",
		Webhook: "http://127.0.0.1:1/hook",
		Events:  "ALL",
	}
	mycli := &MyClient{
		service:            restarts,
		Instance:           instance,
		userID:             instance.Id,
		token:              instance.Token,
		subscriptions:      []string{"ALL"},
		webhookUrl:         instance.Webhook,
		instanceRepository: repo,
		loggerWrapper:      service.loggerWrapper,
		userInfoCache:      service.userInfoCache,
		config:             &config.Config{},
		isProxy:            true,
	}

	startedAt := time.Now()
	mycli.myEventHandler(&events.Disconnected{})

	select {
	case payload := <-producer.ch:
		elapsed := time.Since(startedAt)
		if elapsed > 2*time.Second {
			t.Fatalf("ConnectFailure demorou %v", elapsed)
		}
		text := string(payload)
		if !strings.Contains(text, `"ConnectFailure"`) ||
			!strings.Contains(text, `"reason":"proxy_error:unreachable"`) {
			t.Fatalf("payload inesperado: %s", text)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ConnectFailure não foi emitido em 2 s")
	}
	if repo.reason != "proxy_error:unreachable" {
		t.Fatalf("reason = %q", repo.reason)
	}
	time.Sleep(100 * time.Millisecond)
	if restarts.calls != 0 {
		t.Fatalf("proxy caiu e o fork reiniciou sozinho (%d)", restarts.calls)
	}
	if restarts.retries != 1 {
		t.Fatalf("ciclo de reconexão agendado %d vez(es)", restarts.retries)
	}
}

func TestReconnectClientThroughNewProxy(t *testing.T) {
	container, err := sqlstore.New(
		context.Background(),
		"sqlite",
		"file:"+filepath.Join(t.TempDir(), "reconnect.sqlite")+"?_pragma=foreign_keys(1)",
		nil,
	)
	if err != nil {
		t.Fatalf("sqlstore: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })

	newProxy, _ := json.Marshal(ProxyConfig{
		Protocol: "socks5",
		Host:     "127.0.0.1",
		Port:     "1080",
		Username: "u",
		Password: "p",
	})
	repo := &repoSpy{}
	repo.instance = &instance_model.Instance{
		Id:     "inst-reconnect",
		Token:  "token-reconnect",
		Name:   "Reconnect QA",
		Jid:    "5511999999999@s.whatsapp.net",
		Events: "ALL",
		Proxy:  string(newProxy),
	}
	service := whatsmeowService{
		instanceRepository: repo,
		config: &config.Config{
			LogDirectory:         os.TempDir(),
			WhatsappVersionMajor: 2,
			WhatsappVersionMinor: 3000,
			WhatsappVersionPatch: 1,
		},
		clientPointer:   map[string]*whatsmeow.Client{},
		myClientPointer: map[string]*MyClient{},
		killChannel:     map[string](chan bool){},
		userInfoCache:   cache.New(cache.NoExpiration, 0),
		loggerWrapper:   logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: os.TempDir()}),
		storeContainer:  &storeContainerHolder{container: container},
	}

	connected := make(chan time.Time, 1)
	proxySeen := make(chan string, 1)
	service.connector = func(cd *ClientData, _ *whatsmeow.Client, proxyConfigured bool) error {
		if !proxyConfigured {
			t.Error("reconexão não usou o proxy novo")
		}
		proxySeen <- cd.Instance.Proxy
		connected <- time.Now()
		return nil // dialer falso: handshake simulado, nada vai para o WhatsApp
	}

	startedAt := time.Now()
	done := make(chan error, 1)
	go func() { done <- service.ReconnectClient("inst-reconnect") }()

	select {
	case <-connected:
	case <-time.After(10 * time.Second):
		t.Fatal("cliente não conectou pelo proxy novo em 10 s")
	}
	elapsed := time.Since(startedAt)
	if elapsed > 8*time.Second {
		t.Fatalf("reconexão demorou %v", elapsed)
	}
	if err := <-done; err != nil {
		t.Fatalf("ReconnectClient: %v", err)
	}
	select {
	case got := <-proxySeen:
		if !strings.Contains(got, `"protocol":"socks5"`) ||
			!strings.Contains(got, `"host":"127.0.0.1"`) {
			t.Fatalf("proxy visto = %s", got)
		}
	default:
		t.Fatal("dialer falso não recebeu o proxy")
	}
	_ = repo
}

func TestProxyRetryReconnectsSameProxy(t *testing.T) {
	container, err := sqlstore.New(
		context.Background(),
		"sqlite",
		"file:"+filepath.Join(t.TempDir(), "retry.sqlite")+"?_pragma=foreign_keys(1)",
		nil,
	)
	if err != nil {
		t.Fatalf("sqlstore: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })

	repo := &repoSpy{}
	repo.instance = &instance_model.Instance{
		Id:      "inst-retry",
		Token:   "token-retry",
		Name:    "Retry QA",
		Jid:     "5511999999999@s.whatsapp.net",
		Events:  "ALL",
		Webhook: "http://127.0.0.1:1/hook",
		Proxy:   `{"protocol":"socks5","host":"127.0.0.1","port":"1080"}`,
	}
	producer := &producerSpy{ch: make(chan []byte, 4)}
	service := &whatsmeowService{
		instanceRepository: repo,
		config: &config.Config{
			LogDirectory:         os.TempDir(),
			WhatsappVersionMajor: 2,
			WhatsappVersionMinor: 3000,
			WhatsappVersionPatch: 1,
		},
		clientPointer:     map[string]*whatsmeow.Client{},
		myClientPointer:   map[string]*MyClient{},
		killChannel:       map[string](chan bool){},
		userInfoCache:     cache.New(cache.NoExpiration, 0),
		loggerWrapper:     logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: os.TempDir()}),
		webhookProducer:   producer,
		storeContainer:    &storeContainerHolder{container: container},
		proxyRetryBackoff: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond},
	}
	attempts := 0
	connected := make(chan struct{}, 1)
	service.connector = func(cd *ClientData, _ *whatsmeow.Client, configured bool) error {
		attempts++
		if !configured || !strings.Contains(cd.Instance.Proxy, `"host":"127.0.0.1"`) {
			t.Error("retry não usou o mesmo proxy")
		}
		if attempts == 1 {
			return errors.New("proxy down")
		}
		connected <- struct{}{}
		return nil
	}

	service.StartProxyRetry(repo.instance.Id)

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("retry não reconectou pelo mesmo proxy")
	}
	if attempts != 2 {
		t.Fatalf("tentativas = %d, quer 2 (uma falha + uma ok)", attempts)
	}
	select {
	case payload := <-producer.ch:
		if !strings.Contains(string(payload), `"event":"Connected"`) {
			t.Fatalf("evento de religação inesperado: %s", payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("evento Connected não foi emitido na religação")
	}
	state := service.proxyRegistry()
	state.mu.Lock()
	pending := len(state.cycles)
	state.mu.Unlock()
	if pending != 0 {
		t.Fatalf("ciclo ficou aberto após conectar (%d)", pending)
	}
}

func TestProxyRetryCanceled(t *testing.T) {
	repo := &repoSpy{}
	repo.instance = &instance_model.Instance{
		Id:    "inst-retry-cancel",
		Jid:   "5511999999999@s.whatsapp.net",
		Proxy: `{"protocol":"socks5","host":"127.0.0.1","port":"1080"}`,
	}
	service := &whatsmeowService{
		instanceRepository: repo,
		config:             &config.Config{LogDirectory: os.TempDir()},
		clientPointer:      map[string]*whatsmeow.Client{},
		myClientPointer:    map[string]*MyClient{},
		killChannel:        map[string](chan bool){},
		userInfoCache:      cache.New(cache.NoExpiration, 0),
		loggerWrapper:      logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: os.TempDir()}),
		proxyRetryBackoff:  []time.Duration{200 * time.Millisecond},
	}
	attempts := 0
	service.connector = func(*ClientData, *whatsmeow.Client, bool) error {
		attempts++
		return errors.New("não deveria tentar")
	}

	service.StartProxyRetry(repo.instance.Id)
	time.Sleep(50 * time.Millisecond)
	service.CancelProxyRetry(repo.instance.Id)
	time.Sleep(300 * time.Millisecond)

	if attempts != 0 {
		t.Fatalf("ciclo cancelado ainda tentou %d vez(es)", attempts)
	}
	state := service.proxyRegistry()
	state.mu.Lock()
	pending := len(state.cycles)
	state.mu.Unlock()
	if pending != 0 {
		t.Fatalf("ciclo cancelado continuou no mapa (%d)", pending)
	}
}

func TestProxyRetrySingleCyclePerInstance(t *testing.T) {
	repo := &repoSpy{}
	repo.instance = &instance_model.Instance{
		Id:    "inst-retry-single",
		Jid:   "5511999999999@s.whatsapp.net",
		Proxy: `{"protocol":"socks5","host":"127.0.0.1","port":"1080"}`,
	}
	service := &whatsmeowService{
		instanceRepository: repo,
		config:             &config.Config{LogDirectory: os.TempDir()},
		clientPointer:      map[string]*whatsmeow.Client{},
		myClientPointer:    map[string]*MyClient{},
		killChannel:        map[string](chan bool){},
		userInfoCache:      cache.New(cache.NoExpiration, 0),
		loggerWrapper:      logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: os.TempDir()}),
		proxyRetryBackoff:  []time.Duration{time.Hour},
	}

	service.StartProxyRetry(repo.instance.Id)
	service.StartProxyRetry(repo.instance.Id)
	service.StartProxyRetry(repo.instance.Id)

	state := service.proxyRegistry()
	state.mu.Lock()
	pending := len(state.cycles)
	state.mu.Unlock()
	if pending != 1 {
		t.Fatalf("ciclos abertos = %d, quer 1", pending)
	}
	service.CancelProxyRetry(repo.instance.Id)
}

func TestApplyProxySetupErrorStaysStrict(t *testing.T) {
	client := whatsmeow.NewClient(&store.Device{}, nil)
	repo := &repoSpy{}
	service := newTestService(repo, &producerSpy{ch: make(chan []byte, 1)})
	cd := &ClientData{
		Instance: &instance_model.Instance{Id: "inst-bad", Proxy: "{not-json"},
		IsProxy:  true,
	}

	_, configured, err := service.applyProxy(cd, client)
	if !configured || err == nil {
		t.Fatalf("proxy inválido: configured=%v err=%v", configured, err)
	}
}

// Guarda contra o fallback silencioso voltar: nenhuma rota pode desligar o
// proxy para conectar direto.
func TestSourceHasNoDirectProxyFallback(t *testing.T) {
	raw, err := os.ReadFile("whatsmeow.go")
	if err != nil {
		t.Fatalf("read whatsmeow.go: %v", err)
	}
	source := string(raw)
	for _, forbidden := range []string{
		"SetProxy(nil)",
		`SetMediaHTTPClient(mediaHTTPClient(""))`,
		"Successfully connected without proxy",
	} {
		if strings.Contains(source, forbidden) {
			t.Errorf("fallback direto ainda presente: %q", forbidden)
		}
	}
}

func TestResolveOsName(t *testing.T) {
	if got := resolveOsName("", "Windows"); got != "Windows" {
		t.Errorf("OS_NAME vazio: %q, quer Windows", got)
	}
	if got := resolveOsName("Android", "Windows"); got != "Android" {
		t.Errorf("OsName da instância deve vencer: %q", got)
	}
	if got := resolveOsName("", ""); got == "" {
		t.Error("sem OS_NAME deve cair no sistema do container")
	}
}

type errString string

func (e errString) Error() string { return string(e) }
