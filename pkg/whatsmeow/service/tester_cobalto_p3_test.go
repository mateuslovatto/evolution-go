package whatsmeow_service

// P3 (Cobalto, evo_p3): nenhum cliente que o kill branch decidiu parar pode
// reconectar sozinho. O whatsmeow não reconecta mais no 515
// (DisableLoginAutoReconnect); o pedido vai ao loop do StartClient, serializado
// com o kill branch, que reconecta o MESMO cliente pelo MESMO connect (proxy
// estrito) só enquanto ele é o dono vivo da instância.
// Nada conecta ao WhatsApp: connector falso, sqlite temporário e, no teste de
// proxy, um SOCKS5 falso local que recusa a autenticação.

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/patrickmn/go-cache"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func p3Pair(t *testing.T, mycli *MyClient) {
	t.Helper()
	// O whatsmeow grava o Store.ID no handlePair, antes do PairSuccess.
	jid := types.NewJID("5511977776666", types.DefaultUserServer)
	mycli.WAClient.Store.ID = &jid
	mycli.WAClient.Store.PushName = "Cobalto P3"
}

func p3WaitReady(t *testing.T, h *agulhaHarness, timeout time.Duration) (*MyClient, bool) {
	t.Helper()
	select {
	case mycli := <-h.ready:
		return mycli, true
	case <-time.After(timeout):
		return nil, false
	}
}

func p3Stop(h *agulhaHarness, mycli *MyClient) {
	mycli.stopSession()
	h.service.killChannel["inst-agulha"] <- true
	<-h.startDone
}

// O cliente criado pelo StartClient não tem nenhum caminho de reconexão
// própria no whatsmeow.
func TestCobaltoP3ClienteSemReconexaoPropria(t *testing.T) {
	h := startAgulha(t, false, func(int32) error { return nil })
	mycli := h.waitReady(t)
	if !mycli.WAClient.DisableLoginAutoReconnect {
		t.Fatal("DisableLoginAutoReconnect desligado: o whatsmeow reconecta sozinho no 515")
	}
	if mycli.WAClient.EnableAutoReconnect {
		t.Fatal("EnableAutoReconnect ligado")
	}
	if mycli.relogin == nil {
		t.Fatal("cliente sem canal de relogin")
	}
	p3Stop(h, mycli)
}

// Pareamento normal: 515 → o loop reconecta o mesmo cliente → Connected.
// A queda do socket antigo (o servidor fecha depois do 515) é ignorada.
func TestCobaltoP3Pareamento515ReconectaEChegaAConnected(t *testing.T) {
	h := startAgulha(t, false, func(int32) error { return nil })
	mycli := h.waitReady(t)
	_ = mycli.beginQRRotation()
	p3Pair(t, mycli)
	if !mycli.markPaired() {
		t.Fatal("PairSuccess deveria vencer")
	}

	mycli.myEventHandler(&events.ManualLoginReconnect{})
	mycli.myEventHandler(&events.Disconnected{}) // socket antigo fechado pelo servidor

	again, ok := p3WaitReady(t, h, 5*time.Second)
	if !ok {
		t.Fatal("515 do pareamento não reconectou")
	}
	if again != mycli {
		t.Fatal("515 subiu outro cliente em vez de reconectar o mesmo")
	}
	if n := atomic.LoadInt32(h.connects); n != 2 {
		t.Fatalf("connector chamado %d vez(es), quer 2", n)
	}
	if h.service.myClientPointer["inst-agulha"] != mycli {
		t.Fatal("cliente reconectado não é o dono nos mapas")
	}

	// O whatsmeow despacha Connected depois do login.
	mycli.myEventHandler(&events.Connected{})
	evs := h.events(1500 * time.Millisecond)
	if c := countEvent(evs, "Connected"); c != 1 {
		t.Fatalf("Connected emitido %d vez(es): %v", c, evs)
	}
	if c := countEvent(evs, "Disconnected") + countEvent(evs, "ConnectFailure"); c != 0 {
		t.Fatalf("queda esperada do 515 virou evento: %v", evs)
	}
	if connected, reason, _ := h.repo.snapshot(); !connected || reason != "" {
		t.Fatalf("estado final connected=%v reason=%q", connected, reason)
	}
	select {
	case <-h.startDone:
		t.Fatal("StartClient saiu depois do relogin")
	default:
	}
	p3Stop(h, mycli)
}

// P3: o teardown venceu (qrEnded) e o aparelho pareou atrasado. O 515 que vem
// depois não reconecta, a queda do socket não vira retry/ReconnectClient, e o
// kill para o cliente. Com e sem proxy.
func TestCobaltoP3515DepoisDoFimDoPareamentoNaoReconecta(t *testing.T) {
	for _, proxy := range []bool{false, true} {
		name := "sem_proxy"
		if proxy {
			name = "com_proxy"
		}
		t.Run(name, func(t *testing.T) {
			h := startAgulha(t, proxy, func(int32) error { return nil })
			mycli := h.waitReady(t)
			stop := mycli.beginQRRotation()
			count, ok := mycli.endQRPairing(stop, "")
			if !ok {
				t.Fatal("rotação viva deveria encerrar o pareamento")
			}
			p3Pair(t, mycli) // pareamento atrasado: Store.ID preenchido

			mycli.myEventHandler(&events.ManualLoginReconnect{})
			mycli.myEventHandler(&events.Disconnected{})

			// O loop segue vivo por mais de um ciclo e não pode reconectar.
			if _, reconnected := p3WaitReady(t, h, 2500*time.Millisecond); reconnected {
				t.Fatal("515 reconectou um cliente que está sendo parado")
			}
			if proxyCycleOpen("inst-agulha") {
				t.Fatal("queda de cliente em parada abriu retry de proxy")
			}

			go mycli.finishQRPairing(count, "", "", false)
			select {
			case <-h.startDone:
			case <-time.After(10 * time.Second):
				t.Fatal("kill não parou o cliente")
			}
			// 515 tardio, depois do kill: continua recusado.
			mycli.myEventHandler(&events.ManualLoginReconnect{})
			if _, reconnected := p3WaitReady(t, h, 1500*time.Millisecond); reconnected {
				t.Fatal("515 depois do kill reconectou")
			}
			if n := atomic.LoadInt32(h.connects); n != 1 {
				t.Fatalf("connector chamado %d vez(es), quer 1", n)
			}
			if h.service.clientPointer["inst-agulha"] != nil || h.service.myClientPointer["inst-agulha"] != nil {
				t.Fatal("cliente parado ficou nos mapas")
			}
			evs := h.events(1500 * time.Millisecond)
			if c := countEvent(evs, "ConnectFailure") + countEvent(evs, "Disconnected") + countEvent(evs, "Connected"); c != 0 {
				t.Fatalf("eventos indevidos para cliente em parada: %v", evs)
			}
		})
	}
}

// 515 durante o kill: pedido de relogin já na fila quando o pareamento acaba
// e o kill chega. Qualquer que seja a ordem em que o loop pega os dois, o
// cliente não reconecta.
func TestCobaltoP3515NaFilaDuranteOKill(t *testing.T) {
	for i := 0; i < 5; i++ {
		h := startAgulha(t, false, func(int32) error { return nil })
		mycli := h.waitReady(t)
		_ = mycli.beginQRRotation()

		// O 515 entra na fila e, antes de o loop pegá-lo, o pareamento acaba
		// (teardown). Montado sob qrMu para o loop não processar no meio: se
		// ele pegar o pedido agora, fica no beginRelogin até o unlock e já vê
		// o fim do pareamento.
		mycli.qrMu.Lock()
		mycli.reloginPending = true
		mycli.relogin <- struct{}{}
		mycli.cancelQRRotationLocked()
		mycli.qrEnded = true
		mycli.qrMu.Unlock()

		go mycli.finishQRPairing(0, "", "", false)
		select {
		case <-h.startDone:
		case <-time.After(10 * time.Second):
			t.Fatal("kill não parou o cliente")
		}
		if n := atomic.LoadInt32(h.connects); n != 1 {
			t.Fatalf("rodada %d: connector chamado %d vez(es), quer 1", i, n)
		}
	}
}

// 515 com proxy: o relogin reusa o mesmo cliente e o mesmo connect estrito, e
// disca pelo mesmo proxy (o SOCKS5 falso recebe a conexão). Como o proxy recusa,
// falha fechado: proxy_error, mapas limpos, ConnectFailure e retry pelo mesmo
// proxy (sessão pareada); nunca conexão direta.
func TestCobaltoP3515ComProxyUsaOMesmoProxy(t *testing.T) {
	addr, accepts, socksConnects := fakeSocks5AuthFail(t)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	container, err := sqlstore.New(
		context.Background(),
		"sqlite",
		"file:"+filepath.Join(t.TempDir(), "p3.sqlite")+"?_pragma=foreign_keys(1)",
		nil,
	)
	if err != nil {
		t.Fatalf("sqlstore: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })

	proxyJSON, _ := json.Marshal(ProxyConfig{Protocol: "socks5", Host: host, Port: port, Username: "user", Password: "pass"})
	repo := &repoSpy{}
	producer := &producerSpy{ch: make(chan []byte, 16)}
	service := &whatsmeowService{
		instanceRepository: repo,
		config: &config.Config{
			LogDirectory:         os.TempDir(),
			WhatsappVersionMajor: 2,
			WhatsappVersionMinor: 3000,
			WhatsappVersionPatch: 1,
		},
		clientPointer:      map[string]*whatsmeow.Client{},
		myClientPointer:    map[string]*MyClient{},
		killChannel:        map[string](chan bool){},
		userInfoCache:      cache.New(cache.NoExpiration, 0),
		loggerWrapper:      logger_wrapper.NewLoggerManager(&config.Config{LogDirectory: os.TempDir()}),
		webhookProducer:    producer,
		storeContainer:     &storeContainerHolder{container: container},
		proxyEOFRetryDelay: 10 * time.Millisecond,
	}

	type call struct {
		client          *whatsmeow.Client
		proxyConfigured bool
	}
	calls := make(chan call, 4)
	ready := make(chan *MyClient, 1)
	var n int32
	service.connector = func(cd *ClientData, client *whatsmeow.Client, proxyConfigured bool) error {
		calls <- call{client, proxyConfigured}
		if atomic.AddInt32(&n, 1) == 1 {
			ready <- service.myClientPointer[cd.Instance.Id]
			return nil // primeiro connect simulado: nada sai pela rede
		}
		// Relogin: o caminho estrito real, pelo proxy que o applyProxy deixou no cliente.
		return service.connectStrictWith(cd, client, proxyConfigured, client.Connect)
	}
	instance := &instance_model.Instance{
		Id:      "inst-p3-proxy",
		Token:   "token-p3-proxy",
		Name:    "P3 proxy",
		Webhook: "http://127.0.0.1:1/hook",
		Events:  "ALL",
		Proxy:   string(proxyJSON),
	}
	cd := &ClientData{Instance: instance, IsProxy: true, Subscriptions: []string{"ALL"}}
	service.killChannel[instance.Id] = make(chan bool)
	done := make(chan struct{})
	go func() {
		service.StartClient(cd)
		close(done)
	}()

	var mycli *MyClient
	select {
	case mycli = <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("StartClient não chegou ao connect")
	}
	first := <-calls
	if !first.proxyConfigured || atomic.LoadInt32(accepts) != 0 {
		t.Fatalf("harness: primeiro connect proxy=%v accepts=%d", first.proxyConfigured, atomic.LoadInt32(accepts))
	}
	spy := &reconnectSpy{WhatsmeowService: service}
	mycli.service = spy
	p3Pair(t, mycli)
	if !mycli.markPaired() {
		t.Fatal("PairSuccess deveria vencer")
	}

	mycli.myEventHandler(&events.ManualLoginReconnect{})

	var second call
	select {
	case second = <-calls:
	case <-time.After(5 * time.Second):
		t.Fatal("515 com proxy não reconectou")
	}
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("StartClient não terminou depois da falha do relogin")
	}
	if second.client != first.client || !second.proxyConfigured {
		t.Fatalf("relogin não reusou o mesmo cliente com proxy (mesmo=%v proxy=%v)", second.client == first.client, second.proxyConfigured)
	}
	if atomic.LoadInt32(accepts) == 0 {
		t.Fatal("relogin não discou pelo proxy configurado")
	}
	if atomic.LoadInt32(socksConnects) != 0 {
		t.Fatal("o proxy falso recebeu CONNECT")
	}
	if repo.connected || repo.reason != "proxy_error:auth" {
		t.Fatalf("falha do relogin não ficou fechada: connected=%v reason=%q", repo.connected, repo.reason)
	}
	if service.clientPointer[instance.Id] != nil || service.myClientPointer[instance.Id] != nil {
		t.Fatal("cliente com relogin falho ficou nos mapas")
	}
	if spy.retries != 1 || spy.calls != 0 {
		t.Fatalf("sessão pareada deveria agendar retry pelo mesmo proxy (retries=%d reconnect=%d)", spy.retries, spy.calls)
	}
	deadline := time.After(3 * time.Second)
	for failure := false; !failure; {
		select {
		case p := <-producer.ch:
			failure = strings.Contains(string(p), `"ConnectFailure"`) && strings.Contains(string(p), "proxy_error:auth")
		case <-deadline:
			t.Fatal("ConnectFailure proxy_error:auth não emitido")
		}
	}
}

// Undecryptable 66/67: o reconnect também passa pelo loop (mesmo cliente) e
// é recusado para cliente em parada.
func TestCobaltoP3Undecryptable6667PassaPeloLoop(t *testing.T) {
	h := startAgulha(t, false, func(int32) error { return nil })
	mycli := h.waitReady(t)
	p3Pair(t, mycli)
	evt := &events.UndecryptableMessage{Info: types.MessageInfo{ID: "66ABCDEF"}}

	mycli.myEventHandler(evt)
	again, ok := p3WaitReady(t, h, 5*time.Second)
	if !ok || again != mycli {
		t.Fatalf("66/67 não reconectou o mesmo cliente pelo loop (ok=%v)", ok)
	}

	// Cliente em parada (kill branch já reivindicou): não reconecta.
	mycli.claimKill()
	mycli.myEventHandler(evt)
	if _, reconnected := p3WaitReady(t, h, 2500*time.Millisecond); reconnected {
		t.Fatal("66/67 reconectou cliente em parada")
	}
	if n := atomic.LoadInt32(h.connects); n != 2 {
		t.Fatalf("connector chamado %d vez(es), quer 2", n)
	}
	p3Stop(h, mycli)
}

// R1: sessão encerrada pelo WhatsApp (sessionEnded) não entra na fila de
// relogin.
func TestCobaltoP3SessaoEncerradaRecusaRelogin(t *testing.T) {
	mycli := &MyClient{config: &config.Config{}, relogin: make(chan struct{}, 1)}
	mycli.stopSession()
	if mycli.requestRelogin() {
		t.Fatal("relogin aceito depois de sessionEnded")
	}
	if ok, _ := mycli.beginRelogin(); ok {
		t.Fatal("beginRelogin aceitou sessão encerrada")
	}
	if len(mycli.relogin) != 0 {
		t.Fatal("pedido de relogin ficou na fila")
	}
}

// R1, janela do connect: o evento terminal chega enquanto o loop reconecta.
// Com sucesso, a conexão é desfeita; com falha, nada de ReconnectClient/retry
// (seria QR novo sozinho depois de logout). Nos dois casos o loop segue vivo
// para o kill.
func TestCobaltoP3EventoTerminalDuranteOConnect(t *testing.T) {
	for _, connectFails := range []bool{false, true} {
		name := "connect_ok"
		if connectFails {
			name = "connect_falha"
		}
		t.Run(name, func(t *testing.T) {
			var mycli *MyClient
			h := startAgulha(t, false, func(call int32) error {
				if call == 0 {
					return nil
				}
				mycli.stopSession() // LoggedOut/ban/StreamReplaced durante o connect
				if connectFails {
					return errors.New("device removed")
				}
				return nil
			})
			mycli = h.waitReady(t)
			spy := &reconnectSpy{WhatsmeowService: h.service}
			mycli.service = spy
			p3Pair(t, mycli)

			if !mycli.requestRelogin() {
				t.Fatal("relogin de sessão viva deveria entrar na fila")
			}
			if _, ok := p3WaitReady(t, h, 5*time.Second); !ok {
				t.Fatal("loop não pegou o pedido de relogin")
			}
			time.Sleep(300 * time.Millisecond)
			select {
			case <-h.startDone:
				t.Fatal("loop saiu: o kill pendente não seria tratado")
			default:
			}
			if spy.calls != 0 || spy.retries != 0 {
				t.Fatalf("sessão encerrada foi \"recuperada\" (reconnect=%d retries=%d)", spy.calls, spy.retries)
			}
			if mycli.WAClient.IsConnected() {
				t.Fatal("cliente de sessão encerrada ficou conectado")
			}
			if n := atomic.LoadInt32(h.connects); n != 2 {
				t.Fatalf("connector chamado %d vez(es), quer 2", n)
			}
			h.service.killChannel["inst-agulha"] <- true
			select {
			case <-h.startDone:
			case <-time.After(10 * time.Second):
				t.Fatal("kill não parou o cliente")
			}
			if n := atomic.LoadInt32(h.connects); n != 2 {
				t.Fatalf("kill depois de sessionEnded reiniciou (connector %d vez(es))", n)
			}
		})
	}
}
