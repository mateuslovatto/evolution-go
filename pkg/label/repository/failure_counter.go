package label_repository

import (
	"sync"

	label_model "github.com/evolution-foundation/evolution-go/pkg/label/model"
)

// FailureCounter exposes how many label writes failed per instance. /label/sync
// compares it before and after the full sync: the LabelEdit events are written
// by the whatsmeow event handler, which can only log a failed write, so this is
// how the sync learns that a label was not refreshed and must not be cleaned up.
type FailureCounter interface {
	WriteFailures(instanceID string) uint64
}

type failureCountingRepo struct {
	LabelRepository
	mu       sync.Mutex
	failures map[string]uint64
}

// WithFailureCounter wraps a repository so failed UpsertLabel and
// DeleteLabelByLabelID calls are counted per instance. The same wrapped value
// must be shared by the event handler and the label service.
func WithFailureCounter(repo LabelRepository) LabelRepository {
	return &failureCountingRepo{LabelRepository: repo, failures: map[string]uint64{}}
}

func (r *failureCountingRepo) count(instanceID string, err error) error {
	if err != nil {
		r.mu.Lock()
		r.failures[instanceID]++
		r.mu.Unlock()
	}
	return err
}

func (r *failureCountingRepo) UpsertLabel(label label_model.Label) error {
	return r.count(label.InstanceID, r.LabelRepository.UpsertLabel(label))
}

func (r *failureCountingRepo) DeleteLabelByLabelID(instanceID string, labelID string) error {
	return r.count(instanceID, r.LabelRepository.DeleteLabelByLabelID(instanceID, labelID))
}

func (r *failureCountingRepo) WriteFailures(instanceID string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failures[instanceID]
}
