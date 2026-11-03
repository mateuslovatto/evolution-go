package message_handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	message_service "github.com/evolution-foundation/evolution-go/pkg/message/service"
	whatsmeow_service "github.com/evolution-foundation/evolution-go/pkg/whatsmeow/service"
	"github.com/gin-gonic/gin"
)

func TestWriteDownloadMediaError_TetoVira413(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	writeDownloadMediaError(ctx, &message_service.MediaTooLargeError{Size: 2000, Max: 1000})

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, esperado 413", recorder.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["mediaSize"].(float64) != 2000 || body["mediaMaxBytes"].(float64) != 1000 {
		t.Fatalf("corpo inesperado: %v", body)
	}
}

func TestWriteDownloadMediaError_OutroErroVira500(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	writeDownloadMediaError(ctx, errors.New("boom"))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, esperado 500", recorder.Code)
	}
}

func TestWriteMessageServiceError_SemClienteVira409(t *testing.T) {
	for name, write := range map[string]func(*gin.Context, error){
		"presence":      writeMessageServiceError,
		"downloadmedia": writeDownloadMediaError,
	} {
		t.Run(name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)

			write(ctx, whatsmeow_service.ErrInstanceNotConnected)

			if recorder.Code != http.StatusConflict {
				t.Fatalf("status = %d, esperado 409 (%s)", recorder.Code, recorder.Body.String())
			}
		})
	}
}
