package whatsmeow_service

// QA adversarial (Agulha) do P3: o stream:error 515 que o WhatsApp manda depois
// do pareamento não pode reconectar um cliente que o kill branch decidiu parar.
// Nada sai para o WhatsApp: o cliente usa um SOCKS5 falso local (que conta
// cada dial e recusa a autenticação) e a suíte roda com --network none.

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/evolution-foundation/evolution-go/pkg/passkey/ceremony"
	"github.com/patrickmn/go-cache"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	_ "modernc.org/sqlite"
)

type p3Harness struct {
	*agulhaHarness
	proxyAccepts *int32
	proxyFlags   chan bool // proxyConfigured visto por cada chamada ao connector
}

// startAgulhaP3 é o startAgulha com o proxy da instância apontando para um
// SOCKS5 falso local: todo dial do websocket do whatsmeow passa por ele.
func startAgulhaP3(t *testing.T) *p3Harness {
	t.Helper()
	addr, accepts, _ := fakeSocks5AuthFail(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	proxyJSON, _ := json.Marshal(ProxyConfig{Protocol: "socks5", Host: host, Port: port, Username: "u", Password: "p"})

	container, err := sqlstore.New(context.Background(), "sqlite",
		"file:"+filepath.Join(t.TempDir(), "p3.sqlite")+"?_pragma=foreign_keys(1)", nil)
	if err != nil {
		t.Fatalf("sqlstore: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })

	instance := &instance_model.Instance{
		Id: "inst-agulha", Token: "token-agulha", Name: "Agulha P3",
		Webhook: "http://127.0.0.1:1/hook", Events: "ALL", Proxy: string(proxyJSON),
	}
	repo := &agulhaRepo{instance: instance}
	producer := &producerSpy{ch: make(chan []byte, 32)}
	service := &whatsmeowService{
		instanceRepository: repo,
		config: &config.Config{
			LogDirectory: os.TempDir(), WhatsappVersionMajor: 2,
			WhatsappVersionMinor: 3000, WhatsappVersionPatch: 1, QrcodeMaxCount: 3,
		},
		clientPointer:     map[string]*whatsmeow.Client{},
		myClientPointer:   map[string]*MyClient{},
		killChannel:       map[string](chan bool){},
		userInfoCache:     cache.New(cache.NoExpiration, 0),
		loggerWrapper:     logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: os.TempDir()}),
		webhookProducer:   producer,
		storeContainer:    &storeContainerHolder{container: container},
		proxyRetryBackoff: []time.Duration{10 * time.Millisecond},
		passkeyCeremony:   ceremony.NewStore(), // como em produção (NewWhatsmeowService)
	}
	h := &p3Harness{
		agulhaHarness: &agulhaHarness{
			service: service, repo: repo, producer: producer, connects: new(int32),
			ready: make(chan *MyClient, 4), startDone: make(chan struct{}),
		},
		proxyAccepts: accepts,
		proxyFlags:   make(chan bool, 8),
	}
	service.connector = func(cd *ClientData, _ *whatsmeow.Client, configured bool) error {
		atomic.AddInt32(h.connects, 1)
		select {
		case h.proxyFlags <- configured:
		default:
		}
		h.ready <- service.myClientPointer[cd.Instance.Id]
		return nil
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

func stream515() *waBinary.Node {
	return &waBinary.Node{Tag: "stream:error", Attrs: waBinary.Attrs{"code": "515"}}
}

// latePairing monta o cenário do P3: o teardown do QR venceu (qrEnded), o
// aparelho pareou mesmo assim (Store.ID gravado antes do PairSuccess) e o
// PairSuccess atrasado caiu no keepLatePairing.
func latePairing(t *testing.T, h *p3Harness, mycli *MyClient) int {
	t.Helper()
	mycli.instanceRepository = &lateRepo{agulhaRepo: h.repo}
	stop := mycli.beginQRRotation()
	count, ok := mycli.endQRPairing(stop, "")
	if !ok {
		t.Fatal("harness: o teardown deveria vencer")
	}
	jid := types.NewJID("5511977776666", types.DefaultUserServer)
	mycli.WAClient.Store.ID = &jid
	mycli.myEventHandler(&events.PairSuccess{ID: jid})
	return count
}

func waitStop(t *testing.T, h *p3Harness) {
	t.Helper()
	select {
	case <-h.startDone:
	case <-time.After(15 * time.Second):
		t.Fatal("StartClient não retornou: o kill branch não parou o cliente")
	}
}

// P3, ordem física mais perigosa: o 515 foi lido do socket logo antes do
// Disconnect do kill branch, e a goroutine de religação do whatsmeow só roda
// depois que o kill branch já parou o cliente. Nenhum dial pode acontecer:
// seria uma sessão logada órfã, fora dos mapas e sem handler.
func TestAgulhaP3_515DepoisDoKillNaoReconecta(t *testing.T) {
	h := startAgulhaP3(t)
	mycli := h.waitReady(t)
	count := latePairing(t, h, mycli)
	go mycli.finishQRPairing(count, "", "", false)
	waitStop(t, h)
	if n := atomic.LoadInt32(h.proxyAccepts); n != 0 {
		t.Fatalf("harness: %d dial(s) antes do 515", n)
	}

	mycli.WAClient.DangerousInternals().HandleStreamError(context.Background(), stream515())

	time.Sleep(3 * time.Second)
	if n := atomic.LoadInt32(h.proxyAccepts); n != 0 {
		t.Fatalf("P3: cliente parado pelo kill branch religou sozinho depois do 515 (%d dial(s) pelo proxy)", n)
	}
	if n := atomic.LoadInt32(h.connects); n != 1 {
		t.Fatalf("connector chamado %d vez(es): o fim do pareamento não reinicia", n)
	}
}

// P3, 515 na janela: o fim do pareamento já foi decidido (qrEnded) e o 515
// chega antes de o kill branch rodar. O cliente vai ser parado; religar agora
// só abre uma corrida com o Disconnect do kill branch. Nenhum dial.
func TestAgulhaP3_515NaJanelaDoKillNaoReconecta(t *testing.T) {
	h := startAgulhaP3(t)
	mycli := h.waitReady(t)
	count := latePairing(t, h, mycli)

	mycli.WAClient.DangerousInternals().HandleStreamError(context.Background(), stream515())
	go mycli.finishQRPairing(count, "", "", false)
	waitStop(t, h)

	time.Sleep(2 * time.Second)
	if n := atomic.LoadInt32(h.proxyAccepts); n != 0 {
		t.Fatalf("P3: 515 depois da decisão de parar gerou %d dial(s) de religação", n)
	}
	if n := atomic.LoadInt32(h.connects); n != 1 {
		t.Fatalf("connector chamado %d vez(es)", n)
	}
}

// Regressão obrigatória: no pareamento normal (PairSuccess venceu), o 515
// tem que religar, e pelo mesmo proxy (F1). Vale religação do próprio
// whatsmeow (dial no SOCKS falso) ou do evolution (connector com proxy).
func TestAgulhaP3_515DoPareamentoNormalReligaPeloProxy(t *testing.T) {
	h := startAgulhaP3(t)
	mycli := h.waitReady(t)
	<-h.proxyFlags // start inicial
	mycli.instanceRepository = &lateRepo{agulhaRepo: h.repo}
	_ = mycli.beginQRRotation()
	jid := types.NewJID("5511955554444", types.DefaultUserServer)
	mycli.WAClient.Store.ID = &jid
	mycli.myEventHandler(&events.PairSuccess{ID: jid})
	if ended, _ := mycli.qrPairingEnded(); ended {
		t.Fatal("harness: pareamento normal não pode estar encerrado")
	}

	mycli.WAClient.DangerousInternals().HandleStreamError(context.Background(), stream515())

	deadline := time.Now().Add(8 * time.Second)
	religou := false
	for time.Now().Before(deadline) && !religou {
		if atomic.LoadInt32(h.proxyAccepts) > 0 {
			religou = true // whatsmeow religou pelo transporte com o proxy
		}
		select {
		case configured := <-h.proxyFlags:
			if !configured {
				t.Fatal("religação do 515 sem proxy (F1)")
			}
			religou = true // evolution religou pelo connectStrict
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !religou {
		t.Fatal("515 do pareamento normal não religou: o pareamento novo nunca chegaria a Connected")
	}

	// encerra o loop do teste (sem ler os mapas, que uma religação pelo
	// evolution pode estar escrevendo)
	mycli.stopSession()
	select {
	case h.service.killChannel["inst-agulha"] <- true:
	case <-time.After(5 * time.Second):
	}
}
