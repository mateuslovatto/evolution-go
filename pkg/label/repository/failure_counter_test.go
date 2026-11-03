package label_repository

import (
	"errors"
	"testing"
	"time"

	label_model "github.com/evolution-foundation/evolution-go/pkg/label/model"
)

type stubRepo struct {
	LabelRepository
	err error
}

func (s *stubRepo) UpsertLabel(label_model.Label) error               { return s.err }
func (s *stubRepo) DeleteLabelByLabelID(string, string) error         { return s.err }
func (s *stubRepo) DeleteLabelsUpdatedBefore(string, time.Time) error { return s.err }

func TestFailureCounterCountsPerInstance(t *testing.T) {
	stub := &stubRepo{}
	repo := WithFailureCounter(stub)
	fc, ok := repo.(FailureCounter)
	if !ok {
		t.Fatal("wrapped repo must implement FailureCounter")
	}

	_ = repo.UpsertLabel(label_model.Label{InstanceID: "a", LabelID: "1"})
	if fc.WriteFailures("a") != 0 {
		t.Fatal("success must not count")
	}

	stub.err = errors.New("db down")
	if err := repo.UpsertLabel(label_model.Label{InstanceID: "a", LabelID: "1"}); err == nil {
		t.Fatal("error must be propagated")
	}
	_ = repo.DeleteLabelByLabelID("a", "2")
	_ = repo.UpsertLabel(label_model.Label{InstanceID: "b", LabelID: "1"})
	_ = repo.DeleteLabelsUpdatedBefore("a", time.Now()) // cleanup is not a LabelEdit write

	if got := fc.WriteFailures("a"); got != 2 {
		t.Fatalf("instance a failures=%d want 2", got)
	}
	if got := fc.WriteFailures("b"); got != 1 {
		t.Fatalf("instance b failures=%d want 1", got)
	}
}
