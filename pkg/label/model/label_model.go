package label_model

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Label struct {
	Id           string `json:"id" gorm:"type:uuid;primaryKey"`
	InstanceID   string `json:"instance_id" gorm:"type:uuid"`
	LabelID      string `json:"label_id"`
	LabelName    string `json:"label_name"`
	LabelColor   string `json:"label_color"`
	PredefinedId string `json:"predefined_id"`
	// UpdatedAt is refreshed on every upsert; /label/sync uses it to drop the
	// labels a full sync did not touch. Nullable column (rows before it are NULL).
	UpdatedAt time.Time `json:"updated_at"`
}

func (m *Label) BeforeCreate(tx *gorm.DB) (err error) {
	m.Id = uuid.New().String()
	return
}
