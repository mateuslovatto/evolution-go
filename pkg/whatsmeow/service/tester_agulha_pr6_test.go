package whatsmeow_service

// QA adversarial (Agulha) do PR #6 aplicado sobre a sbx: kill branch com
// qrEnded, restart do kill comum e queda do proxy durante o pareamento por QR.
// Nada conecta ao WhatsApp: connector falso e sqlstore sqlite temporário.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	instance_repository "github.com/evolution-foundation/evolution-go/pkg/instance/repository"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/patrickmn/go-cache"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types/events"
	_ "modernc.org/sqlite"
)

type agulhaRepo struct {
	instance_repository.InstanceRepository
	mu        sync.Mutex
	instance  *instance_model.Instance
	connected bool
	reason    string
	qrcode    string
	qrWrites  int
}

func (r *agulhaRepo) GetInstanceByID(string) (*instance_model.Instance, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *r.instance
	return &cp, nil
}

func (r *agulhaRepo) UpdateConnected(_ string, status bool, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connected = status
	r.reason = reason
	return nil
}

func (r *agulhaRepo) UpdateQrcode(_ string, qr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.qrcode = qr
	r.qrWrites++
	return nil
}

func (r *agulhaRepo) snapshot() (bool, string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.connected, r.reason, r.qrcode
}

type agulhaHarness struct {
	service   *whatsmeowService
	repo      *agulhaRepo
	producer  *producerSpy
	cd        *ClientData
	connects  *int32
	ready     chan *MyClient
	startDone chan struct{}
}

// startAgulha roda o StartClient real numa goroutine. connectErr decide o
// resultado de cada chamada ao connector (índice = número da chamada, 0-based).
func startAgulha(t *testing.T, proxy bool, connectErr func(call int32) error) *agulhaHarness {
	t.Helper()
	container, err := sqlstore.New(
		context.Background(),
		"sqlite",
		"file:"+filepath.Join(t.TempDir(), "agulha.sqlite")+"?_pragma=foreign_keys(1)",
		nil,
	)
	if err != nil {
		t.Fatalf("sqlstore: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })

	instance := &instance_model.Instance{
		Id:      "inst-agulha",
		Token:   "token-agulha",
		Name:    "Agulha QA",
		Webhook: "http://127.0.0.1:1/hook",
		Events:  "ALL",
	}
	if proxy {
		instance.Proxy = `{"protocol":"socks5","host":"127.0.0.1","port":"1080"}`
	}
	repo := &agulhaRepo{instance: instance}
	producer := &producerSpy{ch: make(chan []byte, 16)}
	service := &whatsmeowService{
		instanceRepository: repo,
		config: &config.Config{
			LogDirectory:         os.TempDir(),
			WhatsappVersionMajor: 2,
			WhatsappVersionMinor: 3000,
			WhatsappVersionPatch: 1,
			QrcodeMaxCount:       3,
		},
		clientPointer:     map[string]*whatsmeow.Client{},
		myClientPointer:   map[string]*MyClient{},
		killChannel:       map[string](chan bool){},
		userInfoCache:     cache.New(cache.NoExpiration, 0),
		loggerWrapper:     logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: os.TempDir()}),
		webhookProducer:   producer,
		storeContainer:    &storeContainerHolder{container: container},
		proxyRetryBackoff: []time.Duration{10 * time.Millisecond},
	}
	h := &agulhaHarness{
		service:   service,
		repo:      repo,
		producer:  producer,
		connects:  new(int32),
		ready:     make(chan *MyClient, 4),
		startDone: make(chan struct{}),
	}
	service.connector = func(cd *ClientData, _ *whatsmeow.Client, _ bool) error {
		call := atomic.AddInt32(h.connects, 1) - 1
		// Mesma goroutine do StartClient: ler o mapa aqui não corre.
		h.ready <- service.myClientPointer[cd.Instance.Id]
		return connectErr(call)
	}
	cdInstance := *instance
	h.cd = &ClientData{Instance: &cdInstance, Subscriptions: []string{"ALL"}}
	service.killChannel[instance.Id] = make(chan bool)
	go func() {
		service.StartClient(h.cd)
		close(h.startDone)
	}()
	return h
}

func (h *agulhaHarness) waitReady(t *testing.T) *MyClient {
	t.Helper()
	select {
	case mycli := <-h.ready:
		if mycli == nil {
			t.Fatal("StartClient não registrou o MyClient antes do connect")
		}
		return mycli
	case <-time.After(10 * time.Second):
		t.Fatal("StartClient não chegou ao connect")
	}
	return nil
}

func (h *agulhaHarness) events(timeout time.Duration) []string {
	var out []string
	deadline := time.After(timeout)
	for {
		select {
		case p := <-h.producer.ch:
			out = append(out, string(p))
		case <-deadline:
			return out
		}
	}
}

// Fim do QR por max-count: o kill branch NÃO reinicia (regra dura do contrato),
// limpa mapas e grava o motivo do teardown.
func TestAgulhaPR6QRMaxCountKillNaoReinicia(t *testing.T) {
	h := startAgulha(t, false, func(int32) error { return nil })
	mycli := h.waitReady(t)

	stop := mycli.beginQRRotation()
	teardownDone := make(chan struct{})
	go func() {
		mycli.teardownQR(stop,
			"Maximum QR code count (3) reached",
			"QR code limit reached (3)",
			true)
		close(teardownDone)
	}()

	select {
	case <-teardownDone:
	case <-time.After(10 * time.Second):
		t.Fatal("teardownQR não terminou (kill não consumido)")
	}
	select {
	case <-h.startDone:
	case <-time.After(10 * time.Second):
		t.Fatal("StartClient não retornou após o fim do QR: reiniciou ou ficou preso no loop")
	}
	if n := atomic.LoadInt32(h.connects); n != 1 {
		t.Fatalf("connector chamado %d vez(es); o fim do QR não pode reiniciar o cliente", n)
	}
	if _, ok := h.service.clientPointer["inst-agulha"]; ok {
		t.Fatal("clientPointer ficou com o cliente morto: o próximo /instance/qr não inicia sessão nova")
	}
	if _, ok := h.service.myClientPointer["inst-agulha"]; ok {
		t.Fatal("myClientPointer ficou com o cliente morto: /instance/connect acha que está rodando")
	}
	if _, ok := h.service.killChannel["inst-agulha"]; ok {
		t.Fatal("killChannel do cliente encerrado não foi removido")
	}
	connected, reason, qr := h.repo.snapshot()
	if connected || reason != "QR code limit reached (3)" {
		t.Fatalf("UpdateConnected final = (%v, %q), quer (false, motivo do max-count)", connected, reason)
	}
	if qr != "" {
		t.Fatalf("QR não foi limpo no banco: %q", qr)
	}
	if mycli.beginQRRotation() != nil {
		t.Fatal("events.QR tardio reabriu a rotação depois do fim do pareamento")
	}
	evs := strings.Join(h.events(2*time.Second), "\n")
	if !strings.Contains(evs, `"event":"QRTimeout"`) {
		t.Fatalf("QRTimeout não emitido; eventos: %s", evs)
	}
}

// Regressão da sbx: um kill comum (sem qrEnded e sem sessionEnded) ainda
// reinicia o cliente.
func TestAgulhaPR6KillComumAindaReinicia(t *testing.T) {
	h := startAgulha(t, false, func(call int32) error {
		if call == 0 {
			return nil
		}
		return errors.New("fim do teste") // o restart chega ao connect e para aqui
	})
	h.waitReady(t)
	h.service.killChannel["inst-agulha"] <- true

	select {
	case <-h.startDone:
	case <-time.After(10 * time.Second):
		t.Fatal("StartClient não retornou")
	}
	if n := atomic.LoadInt32(h.connects); n != 2 {
		t.Fatalf("connector chamado %d vez(es), quer 2 (kill comum reinicia)", n)
	}
}

// Kill com sessionEnded (LoggedOut) não reinicia, mesmo sem qrEnded.
func TestAgulhaPR6KillAposLoggedOutNaoReinicia(t *testing.T) {
	h := startAgulha(t, false, func(int32) error { return nil })
	mycli := h.waitReady(t)
	mycli.stopSession()
	h.service.killChannel["inst-agulha"] <- true

	select {
	case <-h.startDone:
	case <-time.After(10 * time.Second):
		t.Fatal("StartClient não retornou")
	}
	if n := atomic.LoadInt32(h.connects); n != 1 {
		t.Fatalf("connector chamado %d vez(es) após sessionEnded", n)
	}
}

// Rotação antiga (superada por um lote novo de events.QR) não pode encerrar o
// pareamento nem sinalizar kill.
func TestAgulhaPR6RotacaoVelhaNaoDerruba(t *testing.T) {
	h := startAgulha(t, false, func(int32) error { return nil })
	mycli := h.waitReady(t)

	old := mycli.beginQRRotation()
	_ = mycli.beginQRRotation()
	done := make(chan struct{})
	go func() {
		mycli.teardownQR(old, "", "", false)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("teardownQR de rotação velha bloqueou")
	}
	select {
	case <-h.startDone:
		t.Fatal("rotação velha derrubou o cliente atual")
	case <-time.After(2500 * time.Millisecond):
	}
	if ended, _ := mycli.qrPairingEnded(); ended {
		t.Fatal("rotação velha marcou qrEnded")
	}
	// encerra o loop do teste
	mycli.stopSession()
	h.service.killChannel["inst-agulha"] <- true
	<-h.startDone
}

// DEFEITO (merge #6 × sbx): com proxy, o socket cai durante o pareamento.
// O #6 para a rotação em silêncio ("quem desconectou cuida da limpeza"), mas o
// ramo de proxy da sbx no Disconnected só emite ConnectFailure e agenda o retry,
// que sai na hora porque Jid == "". Ninguém limpa: o QR velho fica no banco, o
// cliente morto fica nos mapas e o loop do StartClient fica vivo. Na sbx.12 a
// rotação seguia até o fim e o teardownQR limpava tudo em até ~3 min.
// Resultado: /instance/connect vê o MyClient e não inicia, /instance/qr vê o
// cliente e devolve o QR velho do banco — preso até reiniciar o processo.
func TestAgulhaPR6QuedaDoProxyNoQRNaoDeixaInstanciaPresa(t *testing.T) {
	h := startAgulha(t, true, func(int32) error { return nil })
	mycli := h.waitReady(t)
	if !mycli.isProxy {
		t.Fatal("harness: sessão deveria estar com proxy")
	}

	// Rotação em curso com um código publicado no banco.
	_ = mycli.beginQRRotation()
	_ = h.repo.UpdateQrcode("inst-agulha", "data:image/png;base64,AAAA|2@velho")

	// O socket cai (proxy) durante o pareamento.
	mycli.myEventHandler(&events.Disconnected{})

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		_, _, qr := h.repo.snapshot()
		select {
		case <-h.startDone:
			if qr == "" {
				return // limpo e loop encerrado: ok
			}
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	_, reason, qr := h.repo.snapshot()
	t.Errorf("instância presa após queda do proxy no QR: qr no banco=%q, reason=%q, loop do StartClient vivo=%v, myClientPointer presente=%v",
		qr, reason, !isDone(h.startDone), h.service.UpdateInstanceSettings("inst-agulha") == nil)

	// encerra o loop do teste
	mycli.stopSession()
	h.service.killChannel["inst-agulha"] <- true
	<-h.startDone
}

func isDone(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
