package label_service

// QA adversarial (Espinho) — reteste de /label/sync (D2/D3 corrigidos).
// E1 corrigido em etiqueta_fork_fix2: todos os testes rodam normalmente.

import (
	"context"
	"errors"
	"testing"
	"time"

	label_model "github.com/evolution-foundation/evolution-go/pkg/label/model"
	label_repository "github.com/evolution-foundation/evolution-go/pkg/label/repository"
)

// failingRepo deixa o UpsertLabel de uma etiqueta falhar (erro transitório do banco).
type failingRepo struct {
	*memLabelRepo
	failID string
}

func (f *failingRepo) UpsertLabel(l label_model.Label) error {
	if l.LabelID == f.failID {
		return errors.New("db: connection reset")
	}
	return f.memLabelRepo.UpsertLabel(l)
}

func labelIDs(h *syncHarness, instanceID string) map[string]bool {
	labels, _ := h.repo.GetAllLabelsByInstanceID(instanceID)
	out := map[string]bool{}
	for _, l := range labels {
		out[l.LabelID] = true
	}
	return out
}

// Verde: falha no meio do full sync (parte das etiquetas já regravada) não apaga nada
// e libera o cooldown (erro que não é 429).
func TestEspinhoFalhaParcialNaoApagaNadaELiberaCooldown(t *testing.T) {
	h := newSyncHarness(t)
	seedLabels(h, inst.Id, 3) // a, b, c antigas
	h.svc.fullSync = func(ctx context.Context, id string) error {
		h.fetches++
		_ = h.repo.UpsertLabel(label_model.Label{InstanceID: id, LabelID: "a", LabelName: "nova"})
		return errors.New("stream closed mid-sync")
	}
	if err := h.svc.SyncLabels(inst); err == nil {
		t.Fatal("esperava erro")
	}
	if got := labelIDs(h, inst.Id); !(got["a"] && got["b"] && got["c"]) {
		t.Fatalf("falha parcial apagou etiquetas: %v", got)
	}
	if err := h.svc.SyncLabels(inst); errors.Is(err, ErrLabelSyncCooldown) {
		t.Fatal("cooldown não foi liberado depois de falha que não é 429")
	}
}

// Verde: no sucesso, ficam as etiquetas regravadas pelo sync e as que chegaram em tempo
// real durante o sync; saem as não tocadas e as com updated_at NULL (anteriores à coluna).
func TestEspinhoSucessoMantemTempoRealERemoveNullEObsoletas(t *testing.T) {
	h := newSyncHarness(t)
	h.repo.labels = append(h.repo.labels,
		label_model.Label{InstanceID: inst.Id, LabelID: "legado-null"}, // updated_at zero = NULL
	)
	seedLabels(h, inst.Id, 1) // "a" obsoleta
	h.remote = []string{"1", "2"}
	base := h.svc.fullSync
	h.svc.fullSync = func(ctx context.Context, id string) error {
		// LabelEdit em tempo real (outro aparelho) chega durante o full sync.
		_ = h.repo.UpsertLabel(label_model.Label{InstanceID: id, LabelID: "tempo-real", LabelName: "nova"})
		return base(ctx, id)
	}
	if err := h.svc.SyncLabels(inst); err != nil {
		t.Fatal(err)
	}
	got := labelIDs(h, inst.Id)
	if !got["1"] || !got["2"] || !got["tempo-real"] {
		t.Fatalf("faltam etiquetas que deveriam ficar: %v", got)
	}
	if got["a"] || got["legado-null"] {
		t.Fatalf("obsoleta ou NULL não foi removida: %v", got)
	}
}

// Verde: etiqueta regravada exatamente no instante syncStartedAt não é apagada (comparação estrita).
func TestEspinhoUpdatedAtIgualAoInicioNaoApaga(t *testing.T) {
	h := newSyncHarness(t)
	fixed := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	h.svc.clock = func() time.Time { return fixed }
	h.svc.fullSync = func(ctx context.Context, id string) error {
		h.repo.labels = append(h.repo.labels, label_model.Label{InstanceID: id, LabelID: "x", UpdatedAt: fixed})
		return nil
	}
	if err := h.svc.SyncLabels(inst); err != nil {
		t.Fatal(err)
	}
	if !labelIDs(h, inst.Id)["x"] {
		t.Fatal("etiqueta com updated_at == syncStartedAt foi apagada")
	}
}

// E1 · BAIXA: o handler do whatsmeow só LOGA erro do persistLabelEdit e o FetchAppState
// volta nil. Se o upsert de uma etiqueta que EXISTE falhar no meio do full sync (erro
// transitório do banco), a limpeza do sucesso apaga essa etiqueta (updated_at antigo):
// a etiqueta some até o próximo sync (10 min de cooldown).
func TestEspinhoUpsertFalhoNoSyncNaoApagaEtiquetaExistente(t *testing.T) {
	h := newSyncHarness(t)
	_ = h.repo.UpsertLabel(label_model.Label{InstanceID: inst.Id, LabelID: "1", LabelName: "Pago"})
	_ = h.repo.UpsertLabel(label_model.Label{InstanceID: inst.Id, LabelID: "stale", LabelName: "Velha"})
	// Como no main.go: o MESMO repositório com contador é usado pelo handler e pelo serviço.
	shared := label_repository.WithFailureCounter(&failingRepo{memLabelRepo: h.repo, failID: "1"})
	h.svc.labelRepository = shared
	h.remote = []string{"1"}
	h.svc.fullSync = func(ctx context.Context, id string) error {
		for _, labelID := range h.remote {
			// Igual ao persistLabelEdit: erro só vai para o log, o fetch segue com sucesso.
			_ = shared.UpsertLabel(label_model.Label{InstanceID: id, LabelID: labelID, LabelName: "Pago"})
		}
		return nil
	}
	// Correção E1: com falha de gravação no sync, nada é limpo e o erro volta (cooldown liberado).
	if err := h.svc.SyncLabels(inst); !errors.Is(err, ErrLabelSyncIncomplete) {
		t.Fatalf("esperava ErrLabelSyncIncomplete, veio %v", err)
	}
	if got := labelIDs(h, inst.Id); !got["1"] || !got["stale"] {
		t.Errorf("etiqueta sumiu apesar da falha de gravação no sync: %v", got)
	}
	h.svc.labelRepository = label_repository.WithFailureCounter(h.repo) // banco voltou
	h.svc.fullSync = func(ctx context.Context, id string) error {
		return h.svc.labelRepository.UpsertLabel(label_model.Label{InstanceID: id, LabelID: "1", LabelName: "Pago"})
	}
	if err := h.svc.SyncLabels(inst); err != nil {
		t.Fatalf("cooldown deveria ter sido liberado: %v", err)
	}
	if got := labelIDs(h, inst.Id); !got["1"] || got["stale"] {
		t.Errorf("sync limpo deveria manter 1 e remover stale: %v", got)
	}
}
