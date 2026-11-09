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
)

type alreadyAuthenticatedService struct {
	instance_service.InstanceService
}

func (s *alreadyAuthenticatedService) Pair(*instance_service.PairStruct, *instance_model.Instance) (*instance_service.PairReturnStruct, error) {
	return nil, instance_service.ErrInstanceAlreadyAuthenticated
}

func TestFix2PairLogadoResponde409(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := &instanceHandler{
		config:          &config.Config{},
		instanceService: &alreadyAuthenticatedService{},
	}
	instance := &instance_model.Instance{Id: "inst-pair", Name: "Pair QA"}

	body, _ := json.Marshal(map[string]string{"phone": "5511999999999"})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/instance/pair", bytes.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	ctx.Set("instance", instance)

	h.Pair(ctx)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("Pair logado respondeu %d, quer 409; body=%s", recorder.Code, recorder.Body.String())
	}
}
