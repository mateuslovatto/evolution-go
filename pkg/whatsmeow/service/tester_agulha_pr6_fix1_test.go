package whatsmeow_service

// QA adversarial (Agulha) do fix1 (aa02ffe): queda do proxy no pareamento por
// QR encerra o pareamento uma única vez, limpa tudo e não deixa retry.

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types/events"
)

func countEvent(evs []string, name string) int {
	n := 0
	for _, e := range evs {
		if strings.Contains(e, `"event":"`+name+`"`) {
			n++
		}
	}
	return n
}

func proxyCycleOpen(id string) bool {
	state := processProxyRetryRegistry
	state.mu.Lock()
	defer state.mu.Unlock()
	_, ok := state.cycles[id]
	return ok
}

// Queda do proxy no pareamento: mapas limpos, sem restart, sem ciclo de retry,
// ConnectFailure + QRTimeout (uma vez cada) e motivo proxy_error persistido.
func TestAgulhaFix1QuedaNoQRLimpaTudoSemRetry(t *testing.T) {
	h := startAgulha(t, true, func(int32) error { return nil })
	mycli := h.waitReady(t)
	_ = mycli.beginQRRotation()
	_ = h.repo.UpdateQrcode("inst-agulha", "data:image/png;base64,AAAA|2@velho")

	mycli.myEventHandler(&events.Disconnected{})

	select {
	case <-h.startDone:
	case <-time.After(10 * time.Second):
		t.Fatal("StartClient não retornou após a queda no pareamento")
	}
	if n := atomic.LoadInt32(h.connects); n != 1 {
		t.Fatalf("connector chamado %d vez(es): queda no QR não pode reiniciar", n)
	}
	if proxyCycleOpen("inst-agulha") {
		t.Fatal("ciclo de retry de proxy aberto para pareamento sem sessão")
	}
	for name, present := range map[string]bool{
		"clientPointer":   h.service.clientPointer["inst-agulha"] != nil,
		"myClientPointer": h.service.myClientPointer["inst-agulha"] != nil,
		"killChannel":     h.service.killChannel["inst-agulha"] != nil,
	} {
		if present {
			t.Fatalf("%s ficou com o cliente morto", name)
		}
	}
	connected, reason, qr := h.repo.snapshot()
	if connected || reason != "proxy_error:unreachable" || qr != "" {
		t.Fatalf("estado final = (connected=%v, reason=%q, qr=%q)", connected, reason, qr)
	}
	if mycli.beginQRRotation() != nil {
		t.Fatal("events.QR tardio reabriu a rotação após a queda")
	}
	evs := h.events(2 * time.Second)
	if c := countEvent(evs, "ConnectFailure"); c != 1 {
		t.Fatalf("ConnectFailure emitido %d vez(es), quer 1: %v", c, evs)
	}
	if c := countEvent(evs, "QRTimeout"); c != 1 {
		t.Fatalf("QRTimeout emitido %d vez(es), quer 1: %v", c, evs)
	}
	if c := countEvent(evs, "Connected"); c != 0 {
		t.Fatalf("evento Connected indevido no pareamento: %v", evs)
	}
}

// Os dois gatilhos ao mesmo tempo (rotação chega ao max-count e o socket cai):
// exatamente um encerra, um único QRTimeout, nenhum restart, nada preso.
func TestAgulhaFix1GatilhosConcorrentes(t *testing.T) {
	h := startAgulha(t, true, func(int32) error { return nil })
	mycli := h.waitReady(t)
	stop := mycli.beginQRRotation()

	start := make(chan struct{})
	go func() {
		<-start
		mycli.teardownQR(stop, "Maximum QR code count (3) reached", "QR code limit reached (3)", true)
	}()
	go func() {
		<-start
		mycli.myEventHandler(&events.Disconnected{})
	}()
	close(start)

	select {
	case <-h.startDone:
	case <-time.After(15 * time.Second):
		t.Fatal("StartClient não retornou com os dois gatilhos concorrentes")
	}
	if n := atomic.LoadInt32(h.connects); n != 1 {
		t.Fatalf("connector chamado %d vez(es)", n)
	}
	if proxyCycleOpen("inst-agulha") {
		t.Fatal("ciclo de retry aberto")
	}
	_, reason, qr := h.repo.snapshot()
	if qr != "" {
		t.Fatalf("QR não limpo: %q", qr)
	}
	if reason != "QR code limit reached (3)" && reason != "proxy_error:unreachable" {
		t.Fatalf("motivo final inesperado: %q", reason)
	}
	evs := h.events(2 * time.Second)
	if c := countEvent(evs, "QRTimeout"); c != 1 {
		t.Fatalf("QRTimeout emitido %d vez(es), quer exatamente 1: %v", c, evs)
	}
}

// gatedRepo segura o UpdateQrcode("") do finishQRPairing até o teste soltar,
// para abrir de forma determinística a janela entre endQRPairing e signalKill.
type gatedRepo struct {
	*agulhaRepo
	entered chan struct{}
	release chan struct{}
}

func (g *gatedRepo) UpdateQrcode(id string, qr string) error {
	if qr == "" {
		select {
		case g.entered <- struct{}{}:
		default:
		}
		<-g.release
	}
	return g.agulhaRepo.UpdateQrcode(id, qr)
}

// DEFEITO D2: a rotação vence o endQRPairing (fim do QR) e, antes do
// signalKill, o socket cai (o servidor fecha o socket do QR expirado mais ou
// menos no mesmo instante). O Disconnected chama stopQRRotation(), que fecha o
// stop; o signalKill lê "<-stop" como "kill branch já rodando" e devolve true
// SEM mandar o kill. O endQRPairingOnDrop vira no-op (qrEnded já está marcado).
// Ninguém sinaliza: o loop do StartClient fica vivo e o cliente morto fica nos
// mapas, o mesmo estado preso do D1.
func TestAgulhaFix1QuedaDuranteTeardownDaRotacao(t *testing.T) {
	h := startAgulha(t, true, func(int32) error { return nil })
	mycli := h.waitReady(t)
	g := &gatedRepo{agulhaRepo: h.repo, entered: make(chan struct{}, 1), release: make(chan struct{})}
	mycli.instanceRepository = g

	stop := mycli.beginQRRotation()
	go mycli.teardownQR(stop, "", "", false) // fim natural do QR (códigos esgotados)

	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("teardownQR não chegou ao finishQRPairing")
	}
	mycli.myEventHandler(&events.Disconnected{}) // socket cai nesta janela
	close(g.release)

	select {
	case <-h.startDone:
	case <-time.After(qrKillSignalTimeout + 5*time.Second):
		_, reason, qr := h.repo.snapshot()
		t.Errorf("instância presa: kill nunca sinalizado (loop vivo, myClientPointer=%v, qr=%q, reason=%q)",
			h.service.UpdateInstanceSettings("inst-agulha") == nil, qr, reason)
		mycli.stopSession()
		h.service.killChannel["inst-agulha"] <- true
		<-h.startDone
	}
}

// Queda antes de qualquer events.QR (socket conectou e caiu): ainda é
// pareamento, encerra e limpa; o lote QR que chegar depois é ignorado.
func TestAgulhaFix1QuedaAntesDoPrimeiroQR(t *testing.T) {
	h := startAgulha(t, true, func(int32) error { return nil })
	mycli := h.waitReady(t)

	mycli.myEventHandler(&events.Disconnected{})
	select {
	case <-h.startDone:
	case <-time.After(10 * time.Second):
		t.Fatal("StartClient não retornou")
	}
	mycli.handleQRCodes([]string{"2@tardio"})
	time.Sleep(200 * time.Millisecond)
	if _, _, qr := h.repo.snapshot(); qr != "" {
		t.Fatalf("lote QR tardio gravou código de socket morto: %q", qr)
	}
	if n := atomic.LoadInt32(h.connects); n != 1 {
		t.Fatalf("connector chamado %d vez(es)", n)
	}
}

// Depois da queda no pareamento, um novo start (como o StartInstance do
// próximo /instance/qr) sobe um cliente novo normalmente.
func TestAgulhaFix1NovoStartAposQueda(t *testing.T) {
	h := startAgulha(t, true, func(int32) error { return nil })
	mycli := h.waitReady(t)
	mycli.myEventHandler(&events.Disconnected{})
	select {
	case <-h.startDone:
	case <-time.After(10 * time.Second):
		t.Fatal("StartClient não retornou")
	}

	// StartInstance: recria o canal e chama StartClient (a guarda F4 não pode barrar).
	h.service.killChannel["inst-agulha"] = make(chan bool)
	done2 := make(chan struct{})
	go func() {
		h.service.StartClient(h.cd)
		close(done2)
	}()
	mycli2 := h.waitReady(t)
	if mycli2 == mycli {
		t.Fatal("novo start reutilizou o MyClient encerrado")
	}
	if ended, _ := mycli2.qrPairingEnded(); ended {
		t.Fatal("cliente novo já nasceu com qrEnded")
	}
	if mycli2.beginQRRotation() == nil {
		t.Fatal("cliente novo não aceita rotação de QR")
	}
	mycli2.stopSession()
	h.service.killChannel["inst-agulha"] <- true
	select {
	case <-done2:
	case <-time.After(10 * time.Second):
		t.Fatal("segundo StartClient não encerrou")
	}
	if n := atomic.LoadInt32(h.connects); n != 2 {
		t.Fatalf("connector chamado %d vez(es), quer 2", n)
	}
}
