package label_handler

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	label_service "github.com/evolution-foundation/evolution-go/pkg/label/service"
	"github.com/gin-gonic/gin"
)

type chatLabelStub struct {
	label_service.LabelService
	err error
}

func (s *chatLabelStub) ChatLabel(*label_service.ChatLabelStruct, *instance_model.Instance) error {
	return s.err
}

func doChatLabel(svc label_service.LabelService) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/label/chat", func(c *gin.Context) {
		c.Set("instance", &instance_model.Instance{Id: "inst-a"})
	}, NewLabelHandler(svc).ChatLabel)
	w := httptest.NewRecorder()
	body := strings.NewReader(`{"jid":"5511999999999@s.whatsapp.net","labelId":"1"}`)
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/label/chat", body))
	return w
}

func TestChatLabelHandlerSemClienteVira409(t *testing.T) {
	w := doChatLabel(&chatLabelStub{err: label_service.ErrLabelSyncNotConnected})
	if w.Code != http.StatusConflict {
		t.Fatalf("got %d want 409 (%s)", w.Code, w.Body.String())
	}
}

func TestChatLabelHandlerOutroErroVira500(t *testing.T) {
	w := doChatLabel(&chatLabelStub{err: errors.New("boom")})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("got %d want 500 (%s)", w.Code, w.Body.String())
	}
}
