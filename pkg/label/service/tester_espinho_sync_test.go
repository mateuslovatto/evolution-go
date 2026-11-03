package label_service

// QA adversarial (Espinho) — POST /label/sync.
// Os testes de defeito (D1, D2, D2b, D3) foram corrigidos em etiqueta_fork_fix e rodam normalmente.

import (
	"errors"
	"sync"
	"testing"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	label_model "github.com/evolution-foundation/evolution-go/pkg/label/model"
	whatsmeow_service "github.com/evolution-foundation/evolution-go/pkg/whatsmeow/service"
	"go.mau.fi/whatsmeow"
)

func seedLabels(h *syncHarness, instanceID string, n int) {
	for i := 0; i < n; i++ {
		_ = h.repo.UpsertLabel(label_model.Label{InstanceID: instanceID, LabelID: string(rune('a' + i)), LabelName: "antiga"})
	}
}

func countLabels(h *syncHarness, instanceID string) int {
	labels, _ := h.repo.GetAllLabelsByInstanceID(instanceID)
	return len(labels)
}

// D2: o sync apaga TODAS as etiquetas antes do FetchAppState. Se o fetch falhar (timeout,
// queda, IQ de erro), a tabela fica vazia e o cooldown de 10 min já foi consumido: o SBX
// lê zero etiquetas até alguém chamar de novo depois de 10 min.
func TestEspinhoFalhaDoFetchNaoApagaAsEtiquetas(t *testing.T) {
	h := newSyncHarness(t)
	inst := &instance_model.Instance{Id: "inst-a"}
	seedLabels(h, inst.Id, 3)
	h.fetchErr = errors.New("context deadline exceeded")
	if err := h.svc.SyncLabels(inst); err == nil {
		t.Fatal("esperava erro do fetch")
	}
	if got := countLabels(h, inst.Id); got != 3 {
		t.Errorf("fetch falhou e sobraram %d etiquetas (esperado: as 3 antigas preservadas)", got)
	}
}

// D2b: o mesmo com 429. ErrAppStateRateLimited sai do whatsmeow ANTES de buscar qualquer
// patch (backoff ativo), mas a tabela já foi apagada.
func TestEspinho429NaoApagaAsEtiquetas(t *testing.T) {
	h := newSyncHarness(t)
	inst := &instance_model.Instance{Id: "inst-a"}
	seedLabels(h, inst.Id, 3)
	h.fetchErr = whatsmeow.ErrAppStateRateLimited
	err := h.svc.SyncLabels(inst)
	if !errors.Is(err, ErrLabelSyncRateLimited) {
		t.Fatalf("esperava ErrLabelSyncRateLimited, veio %v", err)
	}
	if got := countLabels(h, inst.Id); got != 3 {
		t.Errorf("429 e sobraram %d etiquetas (esperado: as 3 antigas preservadas)", got)
	}
}

type startSpy struct {
	whatsmeow_service.WhatsmeowService
	starts int
}

func (s *startSpy) StartInstance(string) error {
	s.starts++
	return errors.New("spy: não inicia")
}

// D3: a checagem de conexão do sync usa ensureClientConnected, que chama StartInstance
// quando não há cliente em memória: /label/sync numa instância desconectada sobe um
// cliente novo no Evolution (o vetor do incidente de 29/10: StartClient abre pool e,
// sem pareamento, fica em QR).
func TestEspinhoChecagemDeConexaoNaoIniciaCliente(t *testing.T) {
	h := newSyncHarness(t)
	spy := &startSpy{}
	h.svc.clientPointer = map[string]*whatsmeow.Client{}
	h.svc.whatsmeowService = spy
	_ = h.svc.checkConnected("inst-sem-cliente")
	if spy.starts != 0 {
		t.Errorf("checkConnected chamou StartInstance %d vez(es)", spy.starts)
	}
}

// Verde: chamadas concorrentes na mesma instância → um fetch só; as outras recebem cooldown.
func TestEspinhoSyncConcorrenteUmFetch(t *testing.T) {
	h := newSyncHarness(t)
	inst := &instance_model.Instance{Id: "inst-a"}
	var wg sync.WaitGroup
	var mu sync.Mutex
	cooldowns := 0
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if errors.Is(h.svc.SyncLabels(inst), ErrLabelSyncCooldown) {
				mu.Lock()
				cooldowns++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if h.fetches != 1 || cooldowns != 7 {
		t.Fatalf("esperado 1 fetch e 7 cooldowns; veio fetches=%d cooldowns=%d", h.fetches, cooldowns)
	}
}

// Verde: o sync de uma instância não apaga etiquetas de outra.
func TestEspinhoSyncNaoTocaOutraInstancia(t *testing.T) {
	h := newSyncHarness(t)
	seedLabels(h, "inst-b", 2)
	if err := h.svc.SyncLabels(&instance_model.Instance{Id: "inst-a"}); err != nil {
		t.Fatal(err)
	}
	if got := countLabels(h, "inst-b"); got != 2 {
		t.Fatalf("inst-b ficou com %d etiquetas", got)
	}
}
