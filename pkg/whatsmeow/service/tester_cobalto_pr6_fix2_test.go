package whatsmeow_service

// Ciclo de vida do pareamento por QR × kill branch (Cobalto, evo_pr6-fix2).
// O desfecho do pareamento é decidido uma vez, sob qrMu: encerrado (rotação ou
// queda), pareado (PairSuccess) ou reivindicado pelo kill branch. O signalKill
// só para de esperar com a entrega do kill ou com o killStarted do kill branch,
// nunca com o stop da rotação.

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// Depois do claim do kill branch nenhum teardown começa, events.QR tardio é
// ignorado e quem ainda espera no signalKill é liberado sem enviar.
func TestCobaltoFix2KillClaimFechaOPareamento(t *testing.T) {
	mycli := &MyClient{
		config:      &config.Config{},
		WAClient:    cobaltoClient(t, false),
		killCh:      make(chan bool),
		killStarted: make(chan struct{}),
	}
	stop := mycli.beginQRRotation()

	waiting := make(chan bool, 1)
	go func() { waiting <- mycli.signalKill() }()

	if ended, _ := mycli.claimKill(); ended {
		t.Fatal("claim sem fim de pareamento não pode reportar qrEnded")
	}
	select {
	case sent := <-waiting:
		if !sent {
			t.Fatal("signalKill pendente deveria terminar como entregue após o claim")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("signalKill pendente não foi liberado pelo claim")
	}
	if !isDone(stop) {
		t.Fatal("claim não cancelou a rotação")
	}
	if _, ok := mycli.endQRPairing(stop, "x"); ok {
		t.Fatal("teardown começou depois do claim")
	}
	if outcome, _ := mycli.resolveProxyDrop("proxy_error:unreachable"); outcome != proxyDropOwned {
		t.Fatalf("queda depois do claim não pode encerrar o pareamento, outcome=%d", outcome)
	}
	if ended, _ := mycli.qrPairingEnded(); ended {
		t.Fatal("qrEnded marcado depois do claim")
	}
	if mycli.beginQRRotation() != nil {
		t.Fatal("events.QR tardio reabriu a rotação depois do claim")
	}
}

// PairSuccess primeiro: o pareamento não pode mais ser encerrado (nem pela
// rotação, nem pela queda, que vira retry de sessão) e não há qrEnded zumbi.
func TestCobaltoFix2PairSuccessPrimeiroVence(t *testing.T) {
	mycli := &MyClient{config: &config.Config{}, WAClient: cobaltoClient(t, false)}
	stop := mycli.beginQRRotation()

	if !mycli.markPaired() {
		t.Fatal("PairSuccess antes do fim deveria vencer")
	}
	if !isDone(stop) {
		t.Fatal("PairSuccess não cancelou a rotação")
	}
	if _, ok := mycli.endQRPairing(stop, "QR code limit reached (5)"); ok {
		t.Fatal("teardown encerrou um pareamento que já tinha dado certo")
	}
	if outcome, _ := mycli.resolveProxyDrop("proxy_error:unreachable"); outcome != proxyDropRetry {
		t.Fatalf("queda depois do PairSuccess deve seguir o retry de sessão, outcome=%d", outcome)
	}
	if ended, _ := mycli.qrPairingEnded(); ended {
		t.Fatal("sessão pareada ficou com qrEnded (o kill seguinte não reiniciaria)")
	}
	if mycli.beginQRRotation() != nil {
		t.Fatal("events.QR depois do PairSuccess reabriu a rotação")
	}
}

type lateRepo struct {
	*agulhaRepo
	mu      sync.Mutex
	updated *instance_model.Instance
}

func (r *lateRepo) Update(instance *instance_model.Instance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *instance
	r.updated = &cp
	return nil
}

func (r *lateRepo) lastUpdate() *instance_model.Instance {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.updated
}

// PairSuccess depois do fim do pareamento (efeito colateral do D2): o kill é
// entregue, o cliente para sem reiniciar e sem qrEnded zumbi; a instância não é
// reportada como conectada, mas o JID fica salvo para o /instance/connect
// retomar o aparelho.
func TestCobaltoFix2PairSuccessAtrasadoParaSemReiniciar(t *testing.T) {
	h := startAgulha(t, false, func(int32) error { return nil })
	mycli := h.waitReady(t)
	repo := &lateRepo{agulhaRepo: h.repo}
	mycli.instanceRepository = repo

	stop := mycli.beginQRRotation()
	count, ok := mycli.endQRPairing(stop, "")
	if !ok {
		t.Fatal("rotação viva deveria encerrar o pareamento")
	}

	// O aparelho pareou enquanto o teardown era decidido: o whatsmeow grava o
	// Store.ID antes de despachar o PairSuccess.
	jid := types.NewJID("5511988887777", types.DefaultUserServer)
	mycli.WAClient.Store.ID = &jid
	mycli.myEventHandler(&events.PairSuccess{ID: jid})
	mycli.stopQRRotation() // o Disconnected/LoggedOut também fecham o stop: não pode importar

	go mycli.finishQRPairing(count, "", "", false)

	select {
	case <-h.startDone:
	case <-time.After(10 * time.Second):
		t.Fatal("kill não entregue: cliente pareado ficou vivo com qrEnded")
	}
	if n := atomic.LoadInt32(h.connects); n != 1 {
		t.Fatalf("connector chamado %d vez(es): fim do pareamento não reinicia", n)
	}
	if h.service.myClientPointer["inst-agulha"] != nil || h.service.clientPointer["inst-agulha"] != nil {
		t.Fatal("cliente ficou nos mapas")
	}
	up := repo.lastUpdate()
	if up == nil || up.Jid != jid.String() || up.Connected {
		t.Fatalf("JID do pareamento atrasado não salvo como desconectado: %+v", up)
	}
	if connected, _, qr := h.repo.snapshot(); connected || qr != "" {
		t.Fatalf("estado final connected=%v qr=%q", connected, qr)
	}
	evs := h.events(1500 * time.Millisecond)
	if c := countEvent(evs, "PairSuccess"); c != 0 {
		t.Fatalf("PairSuccess reportado para cliente que vai ser parado: %v", evs)
	}
	if c := countEvent(evs, "QRTimeout"); c != 1 {
		t.Fatalf("QRTimeout emitido %d vez(es), quer 1", c)
	}
}

// LoggedOut sinaliza o kill no canal do próprio cliente (não no mapa, que pode
// já ser de um cliente mais novo) e não trava quando o kill branch já o
// reivindicou.
func TestCobaltoFix2LoggedOutUsaOCanalProprio(t *testing.T) {
	repo := &repoSpy{}
	mycli := newEventClient(t, &cancelSpy{}, repo)
	mapCh := mycli.killChannel[mycli.userID] // canal "novo" no mapa (buffer 1)
	mycli.killCh = make(chan bool)
	mycli.killStarted = make(chan struct{})

	done := make(chan struct{})
	go func() {
		mycli.myEventHandler(&events.LoggedOut{})
		close(done)
	}()
	select {
	case <-mycli.killCh:
	case <-time.After(3 * time.Second):
		t.Fatal("LoggedOut não sinalizou o kill do próprio cliente")
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler do LoggedOut não terminou depois da entrega")
	}
	if len(mapCh) != 0 {
		t.Fatal("LoggedOut mandou kill no canal do mapa (cliente mais novo)")
	}

	// Kill branch já reivindicou o cliente: o handler não pode travar.
	claimed := newEventClient(t, &cancelSpy{}, &repoSpy{})
	claimed.killCh = make(chan bool)
	claimed.killStarted = make(chan struct{})
	claimed.claimKill()
	done2 := make(chan struct{})
	go func() {
		claimed.myEventHandler(&events.LoggedOut{})
		close(done2)
	}()
	select {
	case <-done2:
	case <-time.After(3 * time.Second):
		t.Fatal("LoggedOut travou esperando um kill branch que já está rodando")
	}
}

// Sanidade do contrato com a SBX: o QRTimeout do fim por queda carrega o motivo.
func TestCobaltoFix2QRTimeoutDaQuedaTemMotivo(t *testing.T) {
	h := startAgulha(t, true, func(int32) error { return nil })
	mycli := h.waitReady(t)
	mycli.myEventHandler(&events.Disconnected{})
	select {
	case <-h.startDone:
	case <-time.After(10 * time.Second):
		t.Fatal("StartClient não retornou")
	}
	for _, e := range h.events(1500 * time.Millisecond) {
		if strings.Contains(e, `"event":"QRTimeout"`) && !strings.Contains(e, `"reason":"proxy_error:unreachable"`) {
			t.Fatalf("QRTimeout sem o motivo da queda: %s", e)
		}
	}
}
