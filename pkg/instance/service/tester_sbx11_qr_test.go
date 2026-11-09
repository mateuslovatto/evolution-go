package instance_service

import (
	"errors"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	instance_repository "github.com/evolution-foundation/evolution-go/pkg/instance/repository"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/evolution-foundation/evolution-go/pkg/utils"
	whatsmeow_service "github.com/evolution-foundation/evolution-go/pkg/whatsmeow/service"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
)

type whatsmeowSpy struct {
	whatsmeow_service.WhatsmeowService
	starts       int
	updates      int
	cancels      int
	startClients chan struct{}
	reconnects   chan string
}

func (s *whatsmeowSpy) StartProxyRetry(string) {}

func (s *whatsmeowSpy) CancelProxyRetry(string) { s.cancels++ }

func (s *whatsmeowSpy) ClearInstanceCache(string, string) error { return nil }

func (s *whatsmeowSpy) ReconnectClient(instanceId string) error {
	if s.reconnects != nil {
		select {
		case s.reconnects <- instanceId:
		default:
		}
	}
	return nil
}

func (s *whatsmeowSpy) StartInstance(string) error {
	s.starts++
	return errors.New("spy: nao inicia")
}

func (s *whatsmeowSpy) UpdateInstanceSettings(string) error {
	s.updates++
	return errors.New("spy: sem runtime")
}

func (s *whatsmeowSpy) StartClient(*whatsmeow_service.ClientData) {
	if s.startClients != nil {
		select {
		case s.startClients <- struct{}{}:
		default:
		}
	}
}

type instanceRepoSpy struct {
	instance_repository.InstanceRepository
	instance *instance_model.Instance
	gets     int
}

func (r *instanceRepoSpy) Update(instance *instance_model.Instance) error {
	r.instance = instance
	return nil
}

func (r *instanceRepoSpy) GetInstanceByID(string) (*instance_model.Instance, error) {
	r.gets++
	return r.instance, nil
}

func (r *instanceRepoSpy) Delete(string) error { return nil }

// markLoggedIn sets whatsmeow's private isLoggedIn flag. It is test-only: the
// public IsLoggedIn reads the atomic and there is no setter, and this suite
// must never pair or open a real WhatsApp socket.
func markLoggedIn(client *whatsmeow.Client) {
	value := reflect.ValueOf(client).Elem().FieldByName("isLoggedIn")
	(*atomic.Bool)(unsafe.Pointer(value.UnsafeAddr())).Store(true)
}

func newTestInstances(spy *whatsmeowSpy, repo *instanceRepoSpy, clients map[string]*whatsmeow.Client) instances {
	return instances{
		instanceRepository: repo,
		config:             &config.Config{},
		clientPointer:      clients,
		killChannel:        map[string](chan bool){},
		whatsmeowService:   spy,
		loggerWrapper:      logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: os.TempDir()}),
	}
}

func TestQrNaoIniciaComClienteLogado(t *testing.T) {
	client := whatsmeow.NewClient(&store.Device{}, nil)
	markLoggedIn(client)
	instance := &instance_model.Instance{Id: "qr-logged", Events: "ALL"}
	spy := &whatsmeowSpy{}
	repo := &instanceRepoSpy{instance: instance}
	svc := newTestInstances(spy, repo, map[string]*whatsmeow.Client{instance.Id: client})

	_, err := svc.GetQr(instance)
	if err == nil || !strings.Contains(err.Error(), "already logged in") {
		t.Fatalf("erro = %v, quer session already logged in", err)
	}
	if spy.starts != 0 {
		t.Fatalf("QR chamou StartInstance %d vez(es) com cliente logado", spy.starts)
	}
}

func TestQrIniciaSemSessao(t *testing.T) {
	instance := &instance_model.Instance{Id: "qr-empty", Events: "ALL"}
	spy := &whatsmeowSpy{}
	repo := &instanceRepoSpy{instance: instance}
	svc := newTestInstances(spy, repo, map[string]*whatsmeow.Client{})

	_, err := svc.GetQr(instance)
	if err == nil || !strings.Contains(err.Error(), "failed to start instance") {
		t.Fatalf("erro = %v, quer failed to start instance", err)
	}
	if spy.starts != 1 {
		t.Fatalf("StartInstance = %d, quer 1", spy.starts)
	}
}

func TestPairNaoIniciaComClienteLogado(t *testing.T) {
	client := whatsmeow.NewClient(&store.Device{}, nil)
	markLoggedIn(client)
	instance := &instance_model.Instance{Id: "pair-logged", Events: "ALL"}
	spy := &whatsmeowSpy{}
	repo := &instanceRepoSpy{instance: instance}
	svc := newTestInstances(spy, repo, map[string]*whatsmeow.Client{instance.Id: client})

	_, err := svc.Pair(&PairStruct{Phone: "5511999999999"}, instance)
	if err == nil || !strings.Contains(err.Error(), "already authenticated") {
		t.Fatalf("erro = %v, quer already authenticated", err)
	}
	if spy.starts != 0 {
		t.Fatalf("pair chamou StartInstance %d vez(es) com cliente logado", spy.starts)
	}
}

func TestPairIniciaSemSessao(t *testing.T) {
	instance := &instance_model.Instance{Id: "pair-empty", Events: "ALL"}
	spy := &whatsmeowSpy{}
	repo := &instanceRepoSpy{instance: instance}
	svc := newTestInstances(spy, repo, map[string]*whatsmeow.Client{})

	_, err := svc.Pair(&PairStruct{Phone: "5511999999999"}, instance)
	if err == nil || !strings.Contains(err.Error(), "failed to start instance") {
		t.Fatalf("erro = %v, quer failed to start instance", err)
	}
	if spy.starts != 1 {
		t.Fatalf("StartInstance = %d, quer 1", spy.starts)
	}
}

func TestConnectNaoIniciaComClienteLogado(t *testing.T) {
	client := whatsmeow.NewClient(&store.Device{}, nil)
	markLoggedIn(client)
	instance := &instance_model.Instance{Id: "connect-logged", Events: "ALL"}
	spy := &whatsmeowSpy{startClients: make(chan struct{}, 1)}
	repo := &instanceRepoSpy{instance: instance}
	svc := newTestInstances(spy, repo, map[string]*whatsmeow.Client{instance.Id: client})

	if _, _, _, err := svc.Connect(&ConnectStruct{Subscribe: []string{"ALL"}}, instance); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	select {
	case <-spy.startClients:
		t.Fatal("Connect chamou StartClient com cliente logado")
	default:
	}
	if spy.starts != 0 {
		t.Fatalf("Connect chamou StartInstance %d vez(es)", spy.starts)
	}
}

func TestConnectIniciaSemSessao(t *testing.T) {
	instance := &instance_model.Instance{Id: "connect-empty", Events: "ALL"}
	spy := &whatsmeowSpy{startClients: make(chan struct{}, 1)}
	repo := &instanceRepoSpy{instance: instance}
	svc := newTestInstances(spy, repo, map[string]*whatsmeow.Client{})

	if _, _, _, err := svc.Connect(&ConnectStruct{Subscribe: []string{"ALL"}}, instance); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	select {
	case <-spy.startClients:
	case <-timeAfterTest():
		t.Fatal("Connect não iniciou o cliente sem sessão")
	}
}

func TestSetProxyReconectaSemQr(t *testing.T) {
	instance := &instance_model.Instance{
		Id:     "proxy-swap",
		Jid:    "5511999999999@s.whatsapp.net",
		Events: "ALL",
	}
	spy := &whatsmeowSpy{reconnects: make(chan string, 1)}
	repo := &instanceRepoSpy{instance: instance}
	svc := newTestInstances(spy, repo, map[string]*whatsmeow.Client{})

	startedAt := time.Now()
	err := svc.SetProxy(instance.Id, &ProxyConfig{
		Protocol: "socks5",
		Host:     "127.0.0.1",
		Port:     "1080",
		Username: "u",
		Password: "p",
	})
	if err != nil {
		t.Fatalf("SetProxy: %v", err)
	}
	select {
	case id := <-spy.reconnects:
		if id != instance.Id {
			t.Fatalf("reconectou %q", id)
		}
		if elapsed := time.Since(startedAt); elapsed > time.Second {
			t.Fatalf("SetProxy demorou %v para disparar a reconexão", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SetProxy não disparou ReconnectClient")
	}
	if spy.starts != 0 {
		t.Fatalf("SetProxy passou por StartInstance/QR %d vez(es)", spy.starts)
	}
	if spy.cancels != 1 {
		t.Fatalf("SetProxy não cancelou o ciclo antigo (%d)", spy.cancels)
	}
	if !strings.Contains(repo.instance.Proxy, `"protocol":"socks5"`) ||
		!strings.Contains(repo.instance.Proxy, `"host":"127.0.0.1"`) {
		t.Fatalf("proxy salvo = %s", repo.instance.Proxy)
	}
}

func TestRemoveProxyReconectaDireto(t *testing.T) {
	instance := &instance_model.Instance{
		Id:     "proxy-remove",
		Jid:    "5511999999999@s.whatsapp.net",
		Events: "ALL",
		Proxy:  `{"protocol":"socks5","host":"127.0.0.1","port":"1080"}`,
	}
	spy := &whatsmeowSpy{reconnects: make(chan string, 1)}
	repo := &instanceRepoSpy{instance: instance}
	svc := newTestInstances(spy, repo, map[string]*whatsmeow.Client{})

	if err := svc.RemoveProxy(instance.Id); err != nil {
		t.Fatalf("RemoveProxy: %v", err)
	}
	select {
	case <-spy.reconnects:
	case <-time.After(2 * time.Second):
		t.Fatal("RemoveProxy não disparou ReconnectClient")
	}
	if repo.instance.Proxy != "" {
		t.Fatalf("proxy não removido: %q", repo.instance.Proxy)
	}
	if spy.cancels != 1 {
		t.Fatalf("RemoveProxy não cancelou o ciclo (%d)", spy.cancels)
	}
}

func TestLogoutCancelaCiclo(t *testing.T) {
	instance := &instance_model.Instance{Id: "logout-cancel", Events: "ALL"}
	spy := &whatsmeowSpy{}
	repo := &instanceRepoSpy{instance: instance}
	svc := newTestInstances(spy, repo, map[string]*whatsmeow.Client{})

	_, _ = svc.Logout(instance)
	if spy.cancels != 1 {
		t.Fatalf("Logout não cancelou o ciclo (%d)", spy.cancels)
	}
}

func TestDeleteCancelaCiclo(t *testing.T) {
	instance := &instance_model.Instance{Id: "delete-cancel", Events: "ALL"}
	spy := &whatsmeowSpy{}
	repo := &instanceRepoSpy{instance: instance}
	svc := newTestInstances(spy, repo, map[string]*whatsmeow.Client{})

	if err := svc.Delete(instance.Id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if spy.cancels != 1 {
		t.Fatalf("Delete não cancelou o ciclo (%d)", spy.cancels)
	}
}

func TestSetProxyRecusaSocks4(t *testing.T) {
	instance := &instance_model.Instance{Id: "socks4"}
	svc := newTestInstances(&whatsmeowSpy{}, &instanceRepoSpy{instance: instance}, map[string]*whatsmeow.Client{})

	err := svc.SetProxy(instance.Id, &ProxyConfig{Protocol: "socks4", Host: "127.0.0.1", Port: "1080"})
	if !errors.Is(err, utils.ErrSocks4Unsupported) {
		t.Fatalf("erro = %v, quer ErrSocks4Unsupported", err)
	}
	if !strings.Contains(err.Error(), "socks4 não é suportado; use socks5, http ou https") {
		t.Fatalf("mensagem = %q", err.Error())
	}
}

func timeAfterTest() <-chan time.Time {
	ch := make(chan time.Time, 1)
	go func() {
		time.Sleep(2 * time.Second)
		ch <- time.Now()
	}()
	return ch
}
