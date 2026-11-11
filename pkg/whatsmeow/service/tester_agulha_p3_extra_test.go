package whatsmeow_service

// QA adversarial (Agulha) do fix P3 (e67469d): o relogin pelo loop do
// StartClient não pode reconectar uma sessão que o WhatsApp encerrou
// (StreamReplaced, TemporaryBan, LoggedOut => sessionEnded).

import (
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func assertNoRelogin(t *testing.T, h *agulhaHarness, what string) {
	t.Helper()
	time.Sleep(2500 * time.Millisecond) // > 2 ciclos do poll do loop
	if n := atomic.LoadInt32(h.connects); n != 1 {
		t.Errorf("%s: sessão encerrada foi reconectada pelo relogin (connector %d vez(es), quer 1)", what, n)
	}
}

func stopLoop(t *testing.T, h *agulhaHarness, mycli *MyClient) {
	t.Helper()
	mycli.stopSession()
	select {
	case h.service.killChannel["inst-agulha"] <- true:
	case <-h.startDone:
	case <-time.After(5 * time.Second):
	}
	select {
	case <-h.startDone:
	case <-time.After(10 * time.Second):
		t.Fatal("loop do StartClient não terminou")
	}
}

// StreamReplaced (outra sessão assumiu o aparelho) seguido de 515: religar
// derruba a outra sessão e cria o ping-pong de StreamReplaced.
func TestAgulhaP3_515DepoisDeStreamReplacedNaoReconecta(t *testing.T) {
	h := startAgulha(t, false, func(int32) error { return nil })
	mycli := h.waitReady(t)
	jid := types.NewJID("5511933332222", types.DefaultUserServer)
	mycli.WAClient.Store.ID = &jid

	mycli.myEventHandler(&events.StreamReplaced{})
	mycli.myEventHandler(&events.ManualLoginReconnect{})

	assertNoRelogin(t, h, "StreamReplaced + 515")
	stopLoop(t, h, mycli)
}

// Pedido de relogin já na fila (66/67 ou 515) quando chega o TemporaryBan:
// o loop pega o pedido depois e não pode reconectar a sessão banida.
func TestAgulhaP3_ReloginNaFilaDepoisDoBanNaoReconecta(t *testing.T) {
	h := startAgulha(t, false, func(int32) error { return nil })
	mycli := h.waitReady(t)
	jid := types.NewJID("5511933332222", types.DefaultUserServer)
	mycli.WAClient.Store.ID = &jid

	if !mycli.requestRelogin() {
		t.Fatal("harness: relogin de sessão viva deveria entrar na fila")
	}
	mycli.stopSession() // TemporaryBan/StreamReplaced/LoggedOut antes do poll do loop

	assertNoRelogin(t, h, "relogin na fila + ban")
	stopLoop(t, h, mycli)
}
