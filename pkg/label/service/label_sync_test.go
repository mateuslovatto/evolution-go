package label_service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	label_model "github.com/evolution-foundation/evolution-go/pkg/label/model"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"go.mau.fi/whatsmeow"
)

// testClock is a strictly increasing clock shared by the fake repo and the service.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(time.Millisecond)
	return c.t
}

// memLabelRepo mimics the gorm repo: upsert by (instance, label) stamping updated_at.
type memLabelRepo struct {
	mu     sync.Mutex
	clock  *testClock
	labels []label_model.Label
}

func (m *memLabelRepo) InsertLabel(label_model.Label) error             { return nil }
func (m *memLabelRepo) UpdateLabel(label_model.Label) error             { return nil }
func (m *memLabelRepo) GetLabelByID(string) (*label_model.Label, error) { return nil, nil }
func (m *memLabelRepo) DeleteLabel(string) error                        { return nil }
func (m *memLabelRepo) GetAllLabelsByInstanceID(id string) ([]label_model.Label, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []label_model.Label
	for _, l := range m.labels {
		if l.InstanceID == id {
			out = append(out, l)
		}
	}
	return out, nil
}
func (m *memLabelRepo) UpsertLabel(l label_model.Label) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	l.UpdatedAt = m.clock.now()
	for i, cur := range m.labels {
		if cur.InstanceID == l.InstanceID && cur.LabelID == l.LabelID {
			m.labels[i] = l
			return nil
		}
	}
	m.labels = append(m.labels, l)
	return nil
}
func (m *memLabelRepo) DeleteLabelByLabelID(string, string) error { return nil }
func (m *memLabelRepo) DeleteLabelsUpdatedBefore(id string, before time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	kept := m.labels[:0]
	for _, l := range m.labels {
		if l.InstanceID == id && (l.UpdatedAt.IsZero() || l.UpdatedAt.Before(before)) {
			continue
		}
		kept = append(kept, l)
	}
	m.labels = kept
	return nil
}

type syncHarness struct {
	svc      *labelService
	repo     *memLabelRepo
	now      time.Time
	fetches  int
	fetchErr error
	connErr  error
	deadline bool
	remote   []string // label ids the simulated full sync upserts (LabelEdit events)
}

func newSyncHarness(t *testing.T) *syncHarness {
	t.Helper()
	cfg := &config.Config{LogDirectory: t.TempDir()}
	base := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	clock := &testClock{t: base}
	h := &syncHarness{repo: &memLabelRepo{clock: clock}, now: base, remote: []string{"1"}}
	h.svc = &labelService{
		labelRepository: h.repo,
		loggerWrapper:   logger_wrapper.NewLoggerManager(cfg),
		syncCooldown:    newLabelSyncCooldownTracker(labelSyncCooldown),
		clock:           clock.now,
	}
	h.svc.syncCooldown.now = func() time.Time { return h.now }
	h.svc.connectionCheck = func(string) error { return h.connErr }
	h.svc.fullSync = func(ctx context.Context, id string) error {
		h.fetches++
		_, h.deadline = ctx.Deadline()
		if h.fetchErr != nil {
			return h.fetchErr
		}
		for _, labelID := range h.remote {
			if err := h.repo.UpsertLabel(label_model.Label{InstanceID: id, LabelID: labelID, LabelName: "fresh"}); err != nil {
				return err
			}
		}
		return nil
	}
	return h
}

var inst = &instance_model.Instance{Id: "inst-a"}

func TestSyncLabelsRefreshesAndDropsStale(t *testing.T) {
	h := newSyncHarness(t)
	h.remote = []string{"1", "2"}
	_ = h.repo.UpsertLabel(label_model.Label{InstanceID: "inst-a", LabelID: "1", LabelName: "antiga"})
	_ = h.repo.UpsertLabel(label_model.Label{InstanceID: "inst-a", LabelID: "stale"})
	h.repo.labels = append(h.repo.labels, label_model.Label{InstanceID: "inst-a", LabelID: "legacy-null"}) // row from before updated_at
	_ = h.repo.UpsertLabel(label_model.Label{InstanceID: "inst-b", LabelID: "other"})
	if err := h.svc.SyncLabels(inst); err != nil {
		t.Fatal(err)
	}
	got, _ := h.repo.GetAllLabelsByInstanceID("inst-a")
	if len(got) != 2 {
		t.Fatalf("want labels 1 and 2 only, got %+v", got)
	}
	for _, l := range got {
		if l.LabelName != "fresh" || (l.LabelID != "1" && l.LabelID != "2") {
			t.Fatalf("unexpected label %+v", l)
		}
	}
	if other, _ := h.repo.GetAllLabelsByInstanceID("inst-b"); len(other) != 1 {
		t.Fatalf("other instance labels touched: %+v", other)
	}
	if !h.deadline {
		t.Fatal("full sync must run with a timeout")
	}
}

func TestSyncLabelsCooldown(t *testing.T) {
	h := newSyncHarness(t)
	if err := h.svc.SyncLabels(inst); err != nil {
		t.Fatal(err)
	}
	h.now = h.now.Add(9 * time.Minute)
	if err := h.svc.SyncLabels(inst); !errors.Is(err, ErrLabelSyncCooldown) {
		t.Fatalf("want cooldown, got %v", err)
	}
	if err := h.svc.SyncLabels(&instance_model.Instance{Id: "inst-b"}); err != nil {
		t.Fatalf("cooldown must be per instance: %v", err)
	}
	h.now = h.now.Add(1 * time.Minute)
	if err := h.svc.SyncLabels(inst); err != nil {
		t.Fatalf("cooldown must expire after 10 min: %v", err)
	}
	if h.fetches != 3 {
		t.Fatalf("fetches=%d", h.fetches)
	}
}

func TestSyncLabelsRateLimitedKeepsLabelsAndCooldown(t *testing.T) {
	for name, fetchErr := range map[string]error{
		"ErrAppStateRateLimited": fmt.Errorf("x: %w", whatsmeow.ErrAppStateRateLimited),
		"IQError 429":            fmt.Errorf("failed to fetch app state regular patches: %w", &whatsmeow.IQError{Code: 429}),
	} {
		t.Run(name, func(t *testing.T) {
			h := newSyncHarness(t)
			_ = h.repo.UpsertLabel(label_model.Label{InstanceID: "inst-a", LabelID: "old"})
			h.fetchErr = fetchErr
			if err := h.svc.SyncLabels(inst); !errors.Is(err, ErrLabelSyncRateLimited) {
				t.Fatalf("want rate limited, got %v", err)
			}
			if got, _ := h.repo.GetAllLabelsByInstanceID("inst-a"); len(got) != 1 {
				t.Fatalf("429 must keep labels: %+v", got)
			}
			h.fetchErr = nil
			if err := h.svc.SyncLabels(inst); !errors.Is(err, ErrLabelSyncCooldown) {
				t.Fatalf("429 must keep the cooldown, got %v", err)
			}
		})
	}
}

func TestSyncLabelsOtherErrorKeepsLabelsAndReleasesCooldown(t *testing.T) {
	h := newSyncHarness(t)
	_ = h.repo.UpsertLabel(label_model.Label{InstanceID: "inst-a", LabelID: "old"})
	h.fetchErr = errors.New("context deadline exceeded")
	err := h.svc.SyncLabels(inst)
	if err == nil || errors.Is(err, ErrLabelSyncRateLimited) || errors.Is(err, ErrLabelSyncCooldown) {
		t.Fatalf("unexpected %v", err)
	}
	if got, _ := h.repo.GetAllLabelsByInstanceID("inst-a"); len(got) != 1 {
		t.Fatalf("failed fetch must keep labels: %+v", got)
	}
	h.fetchErr = nil
	if err := h.svc.SyncLabels(inst); err != nil {
		t.Fatalf("non-429 failure must release the cooldown: %v", err)
	}
}

func TestSyncLabelsDisconnectedKeepsLabelsAndCooldown(t *testing.T) {
	h := newSyncHarness(t)
	_ = h.repo.UpsertLabel(label_model.Label{InstanceID: "inst-a", LabelID: "old"})
	h.connErr = ErrLabelSyncNotConnected
	if err := h.svc.SyncLabels(inst); !errors.Is(err, ErrLabelSyncNotConnected) {
		t.Fatalf("want not connected, got %v", err)
	}
	if got, _ := h.repo.GetAllLabelsByInstanceID("inst-a"); len(got) != 1 {
		t.Fatalf("labels must not be touched when disconnected: %+v", got)
	}
	if h.fetches != 0 {
		t.Fatal("must not fetch when disconnected")
	}
	h.connErr = nil
	if err := h.svc.SyncLabels(inst); err != nil {
		t.Fatalf("failed connection must not consume the cooldown: %v", err)
	}
}

func TestCheckConnectedWithoutClient(t *testing.T) {
	h := newSyncHarness(t)
	h.svc.clientPointer = map[string]*whatsmeow.Client{}
	if err := h.svc.checkConnected("missing"); !errors.Is(err, ErrLabelSyncNotConnected) {
		t.Fatalf("want not connected, got %v", err)
	}
}
