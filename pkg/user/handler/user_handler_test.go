package user_handler

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	whatsmeow_service "github.com/evolution-foundation/evolution-go/pkg/whatsmeow/service"
	"github.com/gin-gonic/gin"
)

func TestWriteUserServiceError_SemClienteVira409(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	writeUserServiceError(ctx, whatsmeow_service.ErrInstanceNotConnected)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, esperado 409 (%s)", recorder.Code, recorder.Body.String())
	}
}

func TestWriteUserServiceError_OutroErroVira500(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	writeUserServiceError(ctx, errors.New("boom"))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, esperado 500", recorder.Code)
	}
}
