package whatsmeow_service

// PR #6 sobre a sbx (Cobalto, evo_pr6-fix1/fix2): queda do socket com proxy durante
// o pareamento por QR encerra o pareamento pelo mesmo caminho do teardownQR,
// e sessão logada continua com o retry pelo mesmo proxy.

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	instance_repository "github.com/evolution-foundation/evolution-go/pkg/instance/repository"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	_ "modernc.org/sqlite"
)

type cobaltoRepo struct {
	instance_repository.InstanceRepository
	mu       sync.Mutex
	reason   string
	qrcode   string
	qrWrites int
}

func (r *cobaltoRepo) UpdateConnected(_ string, _ bool, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reason = reason
	return nil
}

func (r *cobaltoRepo) UpdateQrcode(_ string, qr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.qrcode = qr
	r.qrWrites++
	return nil
}

func (r *cobaltoRepo) snapshot() (string, string, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reason, r.qrcode, r.qrWrites
}

// cobaltoClient cria um whatsmeow.Client sobre sqlite temporário, sem conectar.
// logged=true simula sessão pareada (Store.ID preenchido).
func cobaltoClient(t *testing.T, logged bool) *whatsmeow.Client {
	t.Helper()
	container, err := sqlstore.New(
		context.Background(),
		"sqlite",
		"file:"+filepath.Join(t.TempDir(), "cobalto.sqlite")+"?_pragma=foreign_keys(1)",
		nil,
	)
	if err != nil {
		t.Fatalf("sqlstore: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })
	device := container.NewDevice()
	if logged {
		jid := types.NewJID("5511999999999", types.DefaultUserServer)
		device.ID = &jid
	}
	return whatsmeow.NewClient(device, nil)
}

func cobaltoMyClient(t *testing.T, logged bool) (*MyClient, *cobaltoRepo, *producerSpy, *reconnectSpy) {
	t.Helper()
	repo := &cobaltoRepo{}
	producer := &producerSpy{ch: make(chan []byte, 8)}
	service := newTestService(&repoSpy{}, producer)
	spy := &reconnectSpy{WhatsmeowService: &service}
	instance := &instance_model.Instance{
		Id:      "inst-cobalto",
		Token:   "token-cobalto",
		Name:    "Cobalto",
		Webhook: "http://127.0.0.1:1/hook",
		Events:  "ALL",
	}
	mycli := &MyClient{
		service:            spy,
		WAClient:           cobaltoClient(t, logged),
		Instance:           instance,
		userID:             instance.Id,
		token:              instance.Token,
		subscriptions:      []string{"ALL"},
		webhookUrl:         instance.Webhook,
		instanceRepository: repo,
		loggerWrapper:      service.loggerWrapper,
		userInfoCache:      service.userInfoCache,
		config:             &config.Config{QrcodeMaxCount: 5},
		isProxy:            true,
	}
	return mycli, repo, producer, spy
}

// Só um dos dois gatilhos encerra o pareamento: a queda do socket com proxy
// (resolveProxyDrop) ou a rotação (endQRPairing). Depois disso, events.QR
// tardio não reabre nada.
func TestCobaltoPR6AbortAndRotationEndPairingOnce(t *testing.T) {
	// Queda primeiro: cancela a rotação viva e a rotação perde a corrida.
	mycli := &MyClient{config: &config.Config{}, WAClient: cobaltoClient(t, false)}
	stop := mycli.beginQRRotation()
	if outcome, _ := mycli.resolveProxyDrop("proxy_error:unreachable"); outcome != proxyDropEndPairing {
		t.Fatalf("queda deveria encerrar o pareamento, outcome=%d", outcome)
	}
	if !isDone(stop) {
		t.Fatal("queda não cancelou a rotação viva")
	}
	if _, ok := mycli.endQRPairing(stop, "QR code limit reached (5)"); ok {
		t.Fatal("rotação cancelada não pode encerrar de novo")
	}
	if outcome, _ := mycli.resolveProxyDrop("outro"); outcome != proxyDropOwned {
		t.Fatalf("queda dupla não pode encerrar de novo, outcome=%d", outcome)
	}
	if mycli.beginQRRotation() != nil {
		t.Fatal("events.QR tardio reabriu a rotação")
	}
	if ended, reason := mycli.qrPairingEnded(); !ended || reason != "proxy_error:unreachable" {
		t.Fatalf("qrPairingEnded = %v, %q", ended, reason)
	}

	// Rotação primeiro: a queda chega tarde e não sobrescreve o motivo.
	mycli = &MyClient{config: &config.Config{}, WAClient: cobaltoClient(t, false)}
	stop = mycli.beginQRRotation()
	if _, ok := mycli.endQRPairing(stop, "QR code limit reached (5)"); !ok {
		t.Fatal("rotação viva deveria encerrar o pareamento")
	}
	if outcome, _ := mycli.resolveProxyDrop("proxy_error:unreachable"); outcome != proxyDropOwned {
		t.Fatalf("queda depois do teardown não pode encerrar de novo, outcome=%d", outcome)
	}
	if _, reason := mycli.qrPairingEnded(); reason != "QR code limit reached (5)" {
		t.Fatalf("motivo sobrescrito: %q", reason)
	}
}

// Queda do proxy no pareamento: ConnectFailure (proxy_error) como na sbx,
// mais QRTimeout com o motivo, QR limpo e nenhum retry (não há sessão).
func TestCobaltoPR6ProxyDropDuringPairingEndsPairingWithoutRetry(t *testing.T) {
	mycli, repo, producer, spy := cobaltoMyClient(t, false)
	stop := mycli.beginQRRotation()
	_ = repo.UpdateQrcode("inst-cobalto", "data:image/png;base64,AAAA|2@velho")

	mycli.myEventHandler(&events.Disconnected{})

	var gotFailure, gotTimeout bool
	deadline := time.After(5 * time.Second)
	for !(gotFailure && gotTimeout) {
		select {
		case p := <-producer.ch:
			text := string(p)
			if strings.Contains(text, `"ConnectFailure"`) && strings.Contains(text, `"reason":"proxy_error:unreachable"`) {
				gotFailure = true
			}
			if strings.Contains(text, `"QRTimeout"`) && strings.Contains(text, `"reason":"proxy_error:unreachable"`) {
				gotTimeout = true
			}
		case <-deadline:
			t.Fatalf("eventos: ConnectFailure=%v QRTimeout=%v", gotFailure, gotTimeout)
		}
	}
	if !isDone(stop) {
		t.Fatal("rotação não foi cancelada")
	}
	if ended, reason := mycli.qrPairingEnded(); !ended || reason != "proxy_error:unreachable" {
		t.Fatalf("qrPairingEnded = %v, %q", ended, reason)
	}
	// O QR some do banco logo depois do QRTimeout.
	for i := 0; i < 50; i++ {
		if _, qr, _ := repo.snapshot(); qr == "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if reason, qr, _ := repo.snapshot(); qr != "" || reason != "proxy_error:unreachable" {
		t.Fatalf("qr=%q reason=%q", qr, reason)
	}
	if spy.retries != 0 || spy.calls != 0 {
		t.Fatalf("pareamento não pode agendar retry/reconnect (retries=%d reconnect=%d)", spy.retries, spy.calls)
	}
}

// Sessão logada com proxy: comportamento da sbx intacto (ConnectFailure e um
// retry pelo mesmo proxy), sem encerrar pareamento nem mexer no QR.
func TestCobaltoPR6ProxyDropLoggedSessionKeepsRetry(t *testing.T) {
	mycli, repo, producer, spy := cobaltoMyClient(t, true)

	mycli.myEventHandler(&events.Disconnected{})

	select {
	case p := <-producer.ch:
		if !strings.Contains(string(p), `"ConnectFailure"`) {
			t.Fatalf("payload inesperado: %s", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ConnectFailure não foi emitido")
	}
	time.Sleep(200 * time.Millisecond)
	if spy.retries != 1 || spy.calls != 0 {
		t.Fatalf("retries=%d reconnect=%d", spy.retries, spy.calls)
	}
	if ended, _ := mycli.qrPairingEnded(); ended {
		t.Fatal("sessão logada não pode marcar fim de pareamento")
	}
	if _, _, writes := repo.snapshot(); writes != 0 {
		t.Fatalf("QR mexido em sessão logada (%d escritas)", writes)
	}
	select {
	case p := <-producer.ch:
		t.Fatalf("evento extra: %s", p)
	default:
	}
}
