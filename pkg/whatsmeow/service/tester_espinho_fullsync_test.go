package whatsmeow_service

// QA adversarial (Espinho) — filtro de full sync.
// Os testes de defeito (D1, D2, D2b, D3) foram corrigidos em etiqueta_fork_fix e rodam normalmente.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func instanceLog(t *testing.T, mycli *MyClient) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(mycli.config.LogDirectory, mycli.userID, "instance.log"))
	if err != nil {
		return ""
	}
	return string(data)
}

// D1: com EmitAppStateEventsOnFullSync=true, o whatsmeow despacha um events.AppState
// genérico para CADA mutação SET do full sync (appstate.go:199-201). Esse tipo não tem
// FromFullSync, passa pelo filtro e cai no case *events.AppState, que loga em INFO o
// %+v inteiro da ação (nome completo e JID do contato, mensagens marcadas etc.) e depois
// o WARN "WEBHOOK SKIPPED". Conta com 5 mil contatos = 10 mil linhas com PII no
// instance.log a cada pareamento ou /label/sync. Antes da flag, nada disso era emitido.
func TestEspinhoAppStateGenericoDoFullSyncNaoLogaPII(t *testing.T) {
	mycli, svc, _ := newTestClient(t)
	for i := 0; i < 200; i++ {
		evt := &events.AppState{
			Index: []string{"contact", "5511999990000@s.whatsapp.net"},
			SyncActionValue: &waSyncAction.SyncActionValue{
				ContactAction: &waSyncAction.ContactAction{FullName: proto.String("Maria Segredo da Silva")},
			},
		}
		if p := runHandler(t, mycli, evt); p != nil {
			t.Fatalf("panic: %v", p)
		}
	}
	if w, g := svc.snapshot(); len(w)+len(g) != 0 {
		t.Fatalf("AppState gerou saída: webhooks=%v globais=%v", w, g)
	}
	log := instanceLog(t, mycli)
	if strings.Contains(log, "Maria Segredo") {
		t.Errorf("instance.log contém o nome do contato vindo do full sync (PII)")
	}
	if lines := strings.Count(log, "\n"); lines > 10 {
		t.Errorf("200 eventos AppState do full sync geraram %d linhas de log (esperado ~0)", lines)
	}
}

// Verde: o AppState genérico continua sem webhook nem fila (só log).
func TestEspinhoAppStateGenericoSemSaida(t *testing.T) {
	mycli, svc, repo := newTestClient(t)
	evt := &events.AppState{Index: []string{"label_edit", "9"}, SyncActionValue: &waSyncAction.SyncActionValue{}}
	if p := runHandler(t, mycli, evt); p != nil {
		t.Fatalf("panic: %v", p)
	}
	if w, g := svc.waitOutputs(1); len(w)+len(g) != 0 {
		t.Fatalf("AppState gerou saída: webhooks=%v globais=%v", w, g)
	}
	if repo.upserts+repo.deletes != 0 {
		t.Fatalf("AppState genérico gravou etiqueta: upserts=%d deletes=%d", repo.upserts, repo.deletes)
	}
}

// Verde: LabelEdit de full sync com Action nil não derruba o handler (getters nil-safe).
func TestEspinhoLabelEditFullSyncActionNil(t *testing.T) {
	mycli, svc, repo := newTestClient(t)
	if p := runHandler(t, mycli, &events.LabelEdit{LabelID: "7", FromFullSync: true}); p != nil {
		t.Fatalf("panic com Action nil: %v", p)
	}
	if w, g := svc.waitOutputs(1); len(w)+len(g) != 0 {
		t.Fatalf("full sync gerou saída: %v %v", w, g)
	}
	if repo.upserts != 1 {
		t.Fatalf("esperado 1 upsert (etiqueta sem nome), veio %d", repo.upserts)
	}
}

// Verde: sequência de full sync que cria e depois apaga a mesma etiqueta termina sem a linha.
func TestEspinhoLabelEditFullSyncCriaEApaga(t *testing.T) {
	mycli, _, repo := newTestClient(t)
	runHandler(t, mycli, labelEdit("5", "Pago", 2, false, true))
	runHandler(t, mycli, labelEdit("5", "Pago", 2, true, true))
	labels, _ := repo.GetAllLabelsByInstanceID(testInstanceID)
	if len(labels) != 0 {
		t.Fatalf("etiqueta apagada no full sync ficou no banco: %+v", labels)
	}
}
