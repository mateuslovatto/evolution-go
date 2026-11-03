package label_service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	label_repository "github.com/evolution-foundation/evolution-go/pkg/label/repository"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
)

const (
	labelSyncCooldown = 10 * time.Minute
	labelSyncTimeout  = 2 * time.Minute
)

var (
	// ErrLabelSyncCooldown is returned when /label/sync is called again for the
	// same instance within labelSyncCooldown.
	ErrLabelSyncCooldown = errors.New("label sync already requested recently, try again later")
	// ErrLabelSyncRateLimited is returned when WhatsApp rate limits the app state fetch.
	ErrLabelSyncRateLimited = errors.New("whatsapp rate limited the label sync, try again later")
	// ErrLabelSyncNotConnected is returned when the instance has no connected, paired client.
	ErrLabelSyncNotConnected = errors.New("instancia nao conectada")
	// ErrLabelSyncIncomplete is returned when some label write failed during the
	// full sync; stale labels are kept and the cooldown is released.
	ErrLabelSyncIncomplete = errors.New("sincronizacao de etiquetas incompleta, etiquetas antigas mantidas")
)

// labelSyncCooldownTracker allows one sync per instance per window.
type labelSyncCooldownTracker struct {
	mu     sync.Mutex
	last   map[string]time.Time
	window time.Duration
	now    func() time.Time
}

func newLabelSyncCooldownTracker(window time.Duration) *labelSyncCooldownTracker {
	return &labelSyncCooldownTracker{last: make(map[string]time.Time), window: window, now: time.Now}
}

// acquire registers an attempt and returns false when still in cooldown.
func (c *labelSyncCooldownTracker) acquire(instanceID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if last, ok := c.last[instanceID]; ok && now.Sub(last) < c.window {
		return false
	}
	c.last[instanceID] = now
	return true
}

// release drops the attempt so it does not count (e.g. client not connected).
func (c *labelSyncCooldownTracker) release(instanceID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.last, instanceID)
}

// checkConnected only inspects the client already in memory. It must never
// start one (StartInstance would open a new session/QR for an unpaired instance).
func (l *labelService) checkConnected(instanceID string) error {
	client := l.clientPointer[instanceID]
	if client == nil || !client.IsConnected() || !client.IsLoggedIn() {
		return ErrLabelSyncNotConnected
	}
	return nil
}

// fetchRegularAppState runs a full sync of the "regular" app state collection,
// where label edits live. Events are dispatched synchronously by whatsmeow, so
// the LabelEdit rows are upserted before this returns.
func (l *labelService) fetchRegularAppState(ctx context.Context, instanceID string) error {
	client := l.clientPointer[instanceID]
	if client == nil {
		return ErrLabelSyncNotConnected
	}
	return client.FetchAppState(ctx, appstate.WAPatchRegular, true, false)
}

func isAppStateRateLimit(err error) bool {
	if errors.Is(err, whatsmeow.ErrAppStateRateLimited) {
		return true
	}
	var iqErr *whatsmeow.IQError
	return errors.As(err, &iqErr) && iqErr.Code == 429
}

// writeFailures reads the shared failure counter (label_repository.WithFailureCounter,
// wired in main.go). A repository without it counts as zero failures.
func (l *labelService) writeFailures(instanceID string) uint64 {
	if fc, ok := l.labelRepository.(label_repository.FailureCounter); ok {
		return fc.WriteFailures(instanceID)
	}
	return 0
}

func (l *labelService) syncNow() time.Time {
	if l.clock != nil {
		return l.clock()
	}
	return time.Now()
}

// SyncLabels refetches the labels with a full app state sync. Nothing is
// deleted up front: the full sync upserts every existing label (refreshing
// updated_at) and only after it succeeds the rows it did not touch are removed.
// A failure keeps the current labels; only a 429 keeps the cooldown.
func (l *labelService) SyncLabels(instance *instance_model.Instance) error {
	if !l.syncCooldown.acquire(instance.Id) {
		return ErrLabelSyncCooldown
	}

	if err := l.connectionCheck(instance.Id); err != nil {
		l.syncCooldown.release(instance.Id)
		return err
	}

	syncStartedAt := l.syncNow()
	failuresBefore := l.writeFailures(instance.Id)

	ctx, cancel := context.WithTimeout(context.Background(), labelSyncTimeout)
	defer cancel()

	if err := l.fullSync(ctx, instance.Id); err != nil {
		l.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error syncing labels: %v", instance.Id, err)
		if isAppStateRateLimit(err) {
			return fmt.Errorf("%w: %v", ErrLabelSyncRateLimited, err)
		}
		l.syncCooldown.release(instance.Id)
		return err
	}

	// A LabelEdit write that failed during the sync left that label with an old
	// updated_at; cleaning up now would delete a label that still exists.
	if failed := l.writeFailures(instance.Id) - failuresBefore; failed > 0 {
		l.loggerWrapper.GetLogger(instance.Id).LogError("[%s] %d label write(s) failed during sync, skipping cleanup", instance.Id, failed)
		l.syncCooldown.release(instance.Id)
		return fmt.Errorf("%w: %d falha(s) de gravacao", ErrLabelSyncIncomplete, failed)
	}

	if err := l.labelRepository.DeleteLabelsUpdatedBefore(instance.Id, syncStartedAt); err != nil {
		l.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error removing stale labels after sync: %v", instance.Id, err)
		return err
	}

	return nil
}
