package instance_handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	config "github.com/evolution-foundation/evolution-go/pkg/config"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	instance_service "github.com/evolution-foundation/evolution-go/pkg/instance/service"
	"github.com/evolution-foundation/evolution-go/pkg/utils"
)

// QA adversarial (Urtiga) do F2: o Create tambem precisa responder 400 para
// socks4, como o SetProxy. Hoje o service devolve ErrSocks4Unsupported e o
// handler traduz qualquer erro em 500.
type createSocks4Service struct {
	instance_service.InstanceService
}

func (s *createSocks4Service) Create(*instance_service.CreateStruct) (*instance_model.Instance, error) {
	return nil, utils.ErrSocks4Unsupported
}

func TestUrtigaCreateSocks4Retorna400(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &instanceHandler{
		config:          &config.Config{},
		instanceService: &createSocks4Service{},
	}

	body, err := json.Marshal(map[string]interface{}{
		"name":  "qa-socks4",
		"token": "qa-token",
		"proxy": map[string]string{
			"protocol": "socks4",
			"host":     "127.0.0.1",
			"port":     "1080",
			"username": "u",
			"password": "p",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/instance/create", bytes.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")

	h.Create(ctx)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("Create com socks4 respondeu %d, quer 400; body=%s", recorder.Code, recorder.Body.String())
	}
}
