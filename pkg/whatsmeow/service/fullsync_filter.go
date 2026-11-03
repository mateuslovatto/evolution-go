package whatsmeow_service

import (
	"fmt"
	"reflect"

	label_model "github.com/evolution-foundation/evolution-go/pkg/label/model"
	label_repository "github.com/evolution-foundation/evolution-go/pkg/label/repository"
	"go.mau.fi/whatsmeow/types/events"
)

// isFromFullSync reports whether an event is an app state event replayed by a
// full sync (FromFullSync == true). The check is generic so app state event
// types added to whatsmeow later are filtered by default.
func isFromFullSync(evt any) bool {
	v := reflect.ValueOf(evt)
	if !v.IsValid() {
		return false
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return false
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return false
	}
	f := v.FieldByName("FromFullSync")
	return f.IsValid() && f.Kind() == reflect.Bool && f.Bool()
}

// applyLabelEdit mirrors a LabelEdit into the labels table: deleted labels are
// removed, any other edit is upserted.
func applyLabelEdit(repo label_repository.LabelRepository, instanceID string, evt *events.LabelEdit) error {
	if evt.Action.GetDeleted() {
		return repo.DeleteLabelByLabelID(instanceID, evt.LabelID)
	}
	return repo.UpsertLabel(label_model.Label{
		InstanceID:   instanceID,
		LabelID:      evt.LabelID,
		LabelName:    evt.Action.GetName(),
		LabelColor:   fmt.Sprintf("%d", evt.Action.GetColor()),
		PredefinedId: fmt.Sprintf("%d", evt.Action.GetPredefinedID()),
	})
}

func (mycli *MyClient) persistLabelEdit(evt *events.LabelEdit) {
	if err := applyLabelEdit(mycli.labelRepository, mycli.userID, evt); err != nil {
		mycli.loggerWrapper.GetLogger(mycli.userID).LogError("[%s] Failed to persist label %s: %v", mycli.userID, evt.LabelID, err)
	}
}
