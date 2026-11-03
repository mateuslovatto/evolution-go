package label_handler

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	label_service "github.com/evolution-foundation/evolution-go/pkg/label/service"
	"github.com/gin-gonic/gin"
)

type stubLabelService struct {
	label_service.LabelService
	err   error
	calls int
}

func (s *stubLabelService) SyncLabels(*instance_model.Instance) error {
	s.calls++
	return s.err
}

func doSync(svc label_service.LabelService, withInstance bool) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/label/sync", func(c *gin.Context) {
		if withInstance {
			c.Set("instance", &instance_model.Instance{Id: "inst-a"})
		}
	}, NewLabelHandler(svc).SyncLabels)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/label/sync", nil))
	return w
}

func TestSyncLabelsHandlerStatus(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"ok", nil, http.StatusOK},
		{"cooldown", label_service.ErrLabelSyncCooldown, http.StatusTooManyRequests},
		{"whatsapp 429", fmt.Errorf("%w: x", label_service.ErrLabelSyncRateLimited), http.StatusTooManyRequests},
		{"not connected", label_service.ErrLabelSyncNotConnected, http.StatusConflict},
		{"other", errors.New("boom"), http.StatusInternalServerError},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if w := doSync(&stubLabelService{err: c.err}, true); w.Code != c.want {
				t.Fatalf("got %d want %d (%s)", w.Code, c.want, w.Body.String())
			}
		})
	}
}

func TestSyncLabelsHandlerWithoutInstance(t *testing.T) {
	svc := &stubLabelService{}
	if w := doSync(svc, false); w.Code != http.StatusUnauthorized || svc.calls != 0 {
		t.Fatalf("got %d calls=%d", w.Code, svc.calls)
	}
}
