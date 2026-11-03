package whatsmeow_service

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestDeclaredMediaInfo(t *testing.T) {
	cases := []struct {
		name     string
		msg      *waE2E.Message
		wantSize int64
		wantMime string
	}{
		{"nil", nil, 0, ""},
		{"text", &waE2E.Message{Conversation: proto.String("oi")}, 0, ""},
		{"image", &waE2E.Message{ImageMessage: &waE2E.ImageMessage{FileLength: proto.Uint64(123)}}, 123, "image/jpeg"},
		{"document uses own mime", &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{FileLength: proto.Uint64(9), Mimetype: proto.String("application/pdf")}}, 9, "application/pdf"},
		{"video without length", &waE2E.Message{VideoMessage: &waE2E.VideoMessage{}}, 0, "video/mp4"},
		{"image wins over video (download order)", &waE2E.Message{
			ImageMessage: &waE2E.ImageMessage{FileLength: proto.Uint64(1)},
			VideoMessage: &waE2E.VideoMessage{FileLength: proto.Uint64(2)},
		}, 1, "image/jpeg"},
		{"associated child video", &waE2E.Message{AssociatedChildMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{VideoMessage: &waE2E.VideoMessage{FileLength: proto.Uint64(77)}},
		}}, 77, "video/mp4"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			size, mime := declaredMediaInfo(c.msg)
			if size != c.wantSize || mime != c.wantMime {
				t.Fatalf("got (%d, %q), want (%d, %q)", size, mime, c.wantSize, c.wantMime)
			}
		})
	}
}

func TestMediaExceedsCap(t *testing.T) {
	const max = 10 * 1024 * 1024
	if mediaExceedsCap(max, max) {
		t.Fatal("exatamente no teto deve embutir")
	}
	if !mediaExceedsCap(max+1, max) {
		t.Fatal("1 byte acima do teto deve omitir")
	}
	if mediaExceedsCap(0, max) {
		t.Fatal("tamanho desconhecido (0) nao omite antes do download")
	}
	if mediaExceedsCap(1<<40, 0) || mediaExceedsCap(1<<40, -1) {
		t.Fatal("teto 0/negativo desliga o corte")
	}
}

// payloadService guarda o corpo dos webhooks; o resto vem do recordingService.
type payloadService struct {
	*recordingService
	pmu      sync.Mutex
	payloads [][]byte
}

func (p *payloadService) CallWebhook(_ *instance_model.Instance, queueName string, data []byte) {
	p.pmu.Lock()
	p.payloads = append(p.payloads, data)
	p.pmu.Unlock()
	p.recordingService.CallWebhook(nil, queueName, data)
}

func (p *payloadService) waitPayload(t *testing.T) map[string]any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.pmu.Lock()
		var body []byte
		if len(p.payloads) > 0 {
			body = p.payloads[0]
		}
		p.pmu.Unlock()
		if body != nil {
			var out map[string]any
			if err := json.Unmarshal(body, &out); err != nil {
				t.Fatalf("payload nao e JSON: %v", err)
			}
			return out
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("nenhum webhook Message emitido")
	return nil
}

// Midia declarada acima do teto: nao baixa (WAClient e nil; um Download
// entraria em panico), nao embute base64 e manda a mensagem com a legenda e
// os campos mediaOmitted/mediaSize.
func TestMessageWebhook_MediaAboveCap_IsOmittedNotDownloaded(t *testing.T) {
	mycli, rec, _ := newTestClient(t)
	mycli.config.WebhookFiles = true
	mycli.config.WebhookMaxMediaBytes = 1024
	svc := &payloadService{recordingService: rec}
	mycli.service = svc

	chat := types.NewJID("5511999999999", types.DefaultUserServer)
	evt := &events.Message{
		Info: types.MessageInfo{
			ID:            "MSGBIG",
			MessageSource: types.MessageSource{Chat: chat, Sender: chat},
			Type:          "media",
		},
		Message: &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
			FileLength: proto.Uint64(20 * 1024 * 1024),
			Mimetype:   proto.String("application/pdf"),
			Caption:    proto.String("contrato assinado"),
			FileName:   proto.String("contrato.pdf"),
		}},
	}

	if p := runHandler(t, mycli, evt); p != nil {
		t.Fatalf("handler entrou em panico (tentou baixar?): %v", p)
	}
	payload := svc.waitPayload(t)

	data, _ := payload["data"].(map[string]any)
	msg, _ := data["Message"].(map[string]any)
	if msg == nil {
		t.Fatalf("payload sem data.Message: %v", payload)
	}
	if msg["mediaOmitted"] != true || msg["mediaOmittedReason"] != "size" {
		t.Fatalf("esperava mediaOmitted=true reason=size, veio %v / %v", msg["mediaOmitted"], msg["mediaOmittedReason"])
	}
	if msg["mediaSize"] != float64(20*1024*1024) || msg["mediaMaxBytes"] != float64(1024) {
		t.Fatalf("mediaSize/mediaMaxBytes errados: %v / %v", msg["mediaSize"], msg["mediaMaxBytes"])
	}
	if msg["mimetype"] != "application/pdf" {
		t.Fatalf("mimetype = %v", msg["mimetype"])
	}
	if _, has := msg["base64"]; has {
		t.Fatal("base64 nao pode ir no webhook acima do teto")
	}
	raw, _ := json.Marshal(msg)
	if !strings.Contains(string(raw), "contrato assinado") {
		t.Fatalf("legenda perdida: %s", raw)
	}
}
