package whatsmeow_service

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	label_model "github.com/evolution-foundation/evolution-go/pkg/label/model"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// recordingService records every output of the event handler. Any other
// WhatsmeowService method hits the nil embedded interface and panics.
type recordingService struct {
	WhatsmeowService
	mu       sync.Mutex
	webhooks []string
	globals  []string
}

func (r *recordingService) CallWebhook(_ *instance_model.Instance, queueName string, _ []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.webhooks = append(r.webhooks, queueName)
}

func (r *recordingService) SendToGlobalQueues(event string, _ []byte, _ string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.globals = append(r.globals, event)
}

func (r *recordingService) snapshot() ([]string, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.webhooks...), append([]string(nil), r.globals...)
}

// waitOutputs waits for the goroutines the handler spawns to report.
func (r *recordingService) waitOutputs(want int) ([]string, []string) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		w, g := r.snapshot()
		if len(w)+len(g) >= want {
			return w, g
		}
		time.Sleep(10 * time.Millisecond)
	}
	return r.snapshot()
}

type fakeLabelRepo struct {
	mu      sync.Mutex
	labels  map[string]label_model.Label // key: instanceID/labelID
	upserts int
	deletes int
}

func newFakeLabelRepo() *fakeLabelRepo {
	return &fakeLabelRepo{labels: map[string]label_model.Label{}}
}

func (f *fakeLabelRepo) key(instanceID, labelID string) string { return instanceID + "/" + labelID }

func (f *fakeLabelRepo) InsertLabel(label_model.Label) error { return errors.New("unexpected") }
func (f *fakeLabelRepo) UpdateLabel(label_model.Label) error { return errors.New("unexpected") }
func (f *fakeLabelRepo) GetLabelByID(string) (*label_model.Label, error) {
	return nil, errors.New("unexpected")
}
func (f *fakeLabelRepo) DeleteLabel(string) error { return errors.New("unexpected") }
func (f *fakeLabelRepo) GetAllLabelsByInstanceID(instanceID string) ([]label_model.Label, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []label_model.Label
	for _, l := range f.labels {
		if l.InstanceID == instanceID {
			out = append(out, l)
		}
	}
	return out, nil
}
func (f *fakeLabelRepo) UpsertLabel(label label_model.Label) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upserts++
	f.labels[f.key(label.InstanceID, label.LabelID)] = label
	return nil
}
func (f *fakeLabelRepo) DeleteLabelByLabelID(instanceID, labelID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes++
	delete(f.labels, f.key(instanceID, labelID))
	return nil
}
func (f *fakeLabelRepo) DeleteLabelsUpdatedBefore(instanceID string, before time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, l := range f.labels {
		if l.InstanceID == instanceID && l.UpdatedAt.Before(before) {
			delete(f.labels, k)
		}
	}
	return nil
}

const testInstanceID = "11111111-1111-1111-1111-111111111111"

// newTestClient builds a MyClient whose only working collaborators are the
// recording service, the fake label repo and the logger. WAClient,
// instanceRepository, producers etc. are nil, so touching them panics.
func newTestClient(t *testing.T) (*MyClient, *recordingService, *fakeLabelRepo) {
	t.Helper()
	cfg := &config.Config{LogDirectory: t.TempDir(), AmqpGlobalEnabled: true, NatsGlobalEnabled: true}
	svc := &recordingService{}
	repo := newFakeLabelRepo()
	return &MyClient{
		service:         svc,
		userID:          testInstanceID,
		Instance:        &instance_model.Instance{Id: testInstanceID, Name: "test"},
		labelRepository: repo,
		config:          cfg,
		loggerWrapper:   logger_wrapper.NewLoggerManager(cfg),
	}, svc, repo
}

func runHandler(t *testing.T, mycli *MyClient, evt any) (panicked any) {
	t.Helper()
	defer func() { panicked = recover() }()
	mycli.myEventHandler(evt)
	return nil
}

func labelEdit(id, name string, color int32, deleted bool, fullSync bool) *events.LabelEdit {
	return &events.LabelEdit{
		LabelID: id,
		Action: &waSyncAction.LabelEditAction{
			Name:         proto.String(name),
			Color:        proto.Int32(color),
			PredefinedID: proto.Int32(3),
			Deleted:      proto.Bool(deleted),
		},
		FromFullSync: fullSync,
	}
}

// Every app state event type in whatsmeow (types/events/appstate.go) that has FromFullSync.
func fullSyncEvents() []any {
	chat := types.NewJID("5511999999999", types.DefaultUserServer)
	return []any{
		&events.Contact{JID: chat, Action: &waSyncAction.ContactAction{FullName: proto.String("x")}, FromFullSync: true},
		&events.Pin{JID: chat, FromFullSync: true},
		&events.Star{ChatJID: chat, FromFullSync: true},
		&events.DeleteForMe{ChatJID: chat, FromFullSync: true},
		&events.Mute{JID: chat, FromFullSync: true},
		&events.Archive{JID: chat, FromFullSync: true},
		&events.MarkChatAsRead{JID: chat, FromFullSync: true},
		&events.ClearChat{JID: chat, FromFullSync: true},
		&events.DeleteChat{JID: chat, FromFullSync: true},
		&events.PushNameSetting{Action: &waSyncAction.PushNameSetting{Name: proto.String("x")}, FromFullSync: true},
		&events.UnarchiveChatsSetting{FromFullSync: true},
		&events.UserStatusMute{JID: chat, FromFullSync: true},
		&events.LabelEdit{LabelID: "1", Action: &waSyncAction.LabelEditAction{Name: proto.String("x")}, FromFullSync: true},
		&events.LabelAssociationChat{JID: chat, LabelID: "1", FromFullSync: true},
		&events.LabelAssociationMessage{JID: chat, LabelID: "1", MessageID: "m", FromFullSync: true},
	}
}

func TestFullSyncEventListCoversAppStateTypes(t *testing.T) {
	evts := fullSyncEvents()
	if len(evts) != 15 {
		t.Fatalf("expected 15 app state event types, got %d", len(evts))
	}
	for _, evt := range evts {
		if !isFromFullSync(evt) {
			t.Errorf("%T: isFromFullSync=false with FromFullSync=true", evt)
		}
	}
}

func TestIsFromFullSync(t *testing.T) {
	type custom struct{ FromFullSync bool }
	type wrongType struct{ FromFullSync string }
	cases := []struct {
		name string
		evt  any
		want bool
	}{
		{"nil", nil, false},
		{"nil pointer", (*events.Archive)(nil), false},
		{"false flag", &events.Archive{}, false},
		{"true flag", &events.Archive{FromFullSync: true}, true},
		{"value struct", events.Archive{FromFullSync: true}, true},
		{"future type with field", &custom{FromFullSync: true}, true},
		{"non bool field", &wrongType{FromFullSync: "true"}, false},
		{"no field", &events.Connected{}, false},
		{"non struct", "x", false},
	}
	for _, c := range cases {
		if got := isFromFullSync(c.evt); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

// Invariant: with EmitAppStateEventsOnFullSync on, full-sync events produce no
// webhook, global queue, DB write (other than labels) or connection update.
func TestFullSyncEventsProduceNoOutputExceptLabels(t *testing.T) {
	for _, evt := range fullSyncEvents() {
		t.Run(fmt.Sprintf("%T", evt), func(t *testing.T) {
			mycli, svc, repo := newTestClient(t)
			if p := runHandler(t, mycli, evt); p != nil {
				t.Fatalf("handler touched a side effect (panic): %v", p)
			}
			time.Sleep(50 * time.Millisecond) // give spawned goroutines a chance
			w, g := svc.snapshot()
			if len(w) != 0 || len(g) != 0 {
				t.Fatalf("unexpected outputs: webhooks=%v globals=%v", w, g)
			}
			_, isLabel := evt.(*events.LabelEdit)
			if !isLabel && (repo.upserts != 0 || repo.deletes != 0) {
				t.Fatalf("unexpected label writes")
			}
			if isLabel && repo.upserts != 1 {
				t.Fatalf("full-sync LabelEdit must be stored, upserts=%d", repo.upserts)
			}
			if mycli.Instance.Connected {
				t.Fatalf("instance marked connected")
			}
		})
	}
}

// PushNameSetting shares the Connected case; from full sync it must return
// before UpdateConnected/presence/webhook. WAClient is nil here, so reaching
// that case would panic on mycli.WAClient.Store.
func TestFullSyncPushNameSettingSkipsConnectedPath(t *testing.T) {
	mycli, svc, _ := newTestClient(t)
	evt := &events.PushNameSetting{Action: &waSyncAction.PushNameSetting{Name: proto.String("Lovatto")}, FromFullSync: true}
	if p := runHandler(t, mycli, evt); p != nil {
		t.Fatalf("reached Connected path: %v", p)
	}
	time.Sleep(50 * time.Millisecond)
	if w, g := svc.snapshot(); len(w)+len(g) != 0 {
		t.Fatalf("unexpected outputs: %v %v", w, g)
	}
}

func TestLabelEditFullSyncUpsertsUpdatesAndDeletes(t *testing.T) {
	mycli, svc, repo := newTestClient(t)

	runHandler(t, mycli, labelEdit("7", "Novo cliente", 2, false, true))
	runHandler(t, mycli, labelEdit("7", "Cliente quente", 5, false, true))
	labels, _ := repo.GetAllLabelsByInstanceID(testInstanceID)
	if len(labels) != 1 {
		t.Fatalf("want 1 label, got %d", len(labels))
	}
	l := labels[0]
	if l.LabelID != "7" || l.LabelName != "Cliente quente" || l.LabelColor != "5" || l.PredefinedId != "3" {
		t.Fatalf("unexpected label %+v", l)
	}

	runHandler(t, mycli, labelEdit("7", "Cliente quente", 5, true, true))
	if labels, _ := repo.GetAllLabelsByInstanceID(testInstanceID); len(labels) != 0 {
		t.Fatalf("deleted label still stored: %+v", labels)
	}
	time.Sleep(50 * time.Millisecond)
	if w, g := svc.snapshot(); len(w)+len(g) != 0 {
		t.Fatalf("full-sync LabelEdit must not emit: %v %v", w, g)
	}
}

// Regression: events outside full sync keep their current behavior.
func TestLabelEditOutsideFullSyncStoresAndEmits(t *testing.T) {
	mycli, svc, repo := newTestClient(t)
	runHandler(t, mycli, labelEdit("9", "Pago", 1, false, false))
	w, g := svc.waitOutputs(2)
	if len(w) != 1 || !strings.HasSuffix(w[0], ".labeledit") || len(g) != 1 || g[0] != "LabelEdit" {
		t.Fatalf("want LabelEdit webhook+global, got %v %v", w, g)
	}
	if labels, _ := repo.GetAllLabelsByInstanceID(testInstanceID); len(labels) != 1 || labels[0].LabelColor != "1" {
		t.Fatalf("label not stored: %+v", labels)
	}

	runHandler(t, mycli, labelEdit("9", "Pago", 1, true, false))
	if labels, _ := repo.GetAllLabelsByInstanceID(testInstanceID); len(labels) != 0 {
		t.Fatalf("deleted label still stored: %+v", labels)
	}
	if w, _ := svc.waitOutputs(4); len(w) != 2 {
		t.Fatalf("deleted LabelEdit must still emit webhook, got %v", w)
	}
}

func TestContactOutsideFullSyncEmits(t *testing.T) {
	mycli, svc, _ := newTestClient(t)
	chat := types.NewJID("5511999999999", types.DefaultUserServer)
	runHandler(t, mycli, &events.Contact{JID: chat, Action: &waSyncAction.ContactAction{FullName: proto.String("x")}})
	w, g := svc.waitOutputs(2)
	if len(w) != 1 || !strings.HasSuffix(w[0], ".contact") || len(g) != 1 || g[0] != "Contact" {
		t.Fatalf("want Contact webhook+global, got %v %v", w, g)
	}
}

// Archive outside full sync reaches its case exactly as before this change.
// That case asserts postMap["data"] to a map while it holds *events.Archive, so
// it panics (whatsmeow recovers handler panics) and emits nothing. This is
// pre-existing behavior, pinned here so the filter does not alter it.
func TestArchiveOutsideFullSyncUnchanged(t *testing.T) {
	mycli, svc, _ := newTestClient(t)
	p := runHandler(t, mycli, &events.Archive{JID: types.NewJID("5511999999999", types.DefaultUserServer)})
	if p == nil || !strings.Contains(fmt.Sprint(p), "interface conversion") {
		t.Fatalf("expected the pre-existing Archive type assertion panic, got %v", p)
	}
	time.Sleep(50 * time.Millisecond)
	if w, g := svc.snapshot(); len(w)+len(g) != 0 {
		t.Fatalf("unexpected outputs: %v %v", w, g)
	}
}

func TestApplyLabelEditNilAction(t *testing.T) {
	repo := newFakeLabelRepo()
	if err := applyLabelEdit(repo, testInstanceID, &events.LabelEdit{LabelID: "1"}); err != nil {
		t.Fatal(err)
	}
	labels, _ := repo.GetAllLabelsByInstanceID(testInstanceID)
	if len(labels) != 1 || labels[0].LabelColor != "0" {
		t.Fatalf("unexpected %+v", labels)
	}
}
