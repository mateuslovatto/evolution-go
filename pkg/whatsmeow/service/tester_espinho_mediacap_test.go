package whatsmeow_service

// QA adversarial (Espinho) — teto de mídia embutida no webhook (WEBHOOK_MAX_MEDIA_BYTES).
// O WAClient do harness é nil: o Download devolve erro e o handler loga "Failed to
// download media"; esse log prova que o teto não cortou antes do download.

import (
	"encoding/json"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

const espinhoCap = 10 * 1024 * 1024

func espinhoClient(t *testing.T, maxBytes int64, minio, files bool) (*MyClient, *payloadService) {
	t.Helper()
	mycli, rec, _ := newTestClient(t)
	mycli.config.WebhookFiles = files
	mycli.config.MinioEnabled = minio
	mycli.config.WebhookMaxMediaBytes = maxBytes
	svc := &payloadService{recordingService: rec}
	mycli.service = svc
	return mycli, svc
}

func espinhoMsg(id string, m *waE2E.Message) *events.Message {
	chat := types.NewJID("5511988887777", types.DefaultUserServer)
	return &events.Message{
		Info: types.MessageInfo{
			ID:            id,
			MessageSource: types.MessageSource{Chat: chat, Sender: chat},
			Type:          "media",
		},
		Message: m,
	}
}

func espinhoDataMessage(t *testing.T, svc *payloadService) map[string]any {
	t.Helper()
	payload := svc.waitPayload(t)
	data, _ := payload["data"].(map[string]any)
	msg, _ := data["Message"].(map[string]any)
	if msg == nil {
		t.Fatalf("payload sem data.Message: %v", payload)
	}
	return msg
}

// Todos os tipos de mídia declarados acima do teto: não baixa, não embute, marca
// mediaOmitted e preserva legenda e o bloco original da mídia (o SBX detecta o tipo por ele).
func TestEspinhoTodosOsTiposAcimaDoTetoSaoOmitidos(t *testing.T) {
	big := proto.Uint64(espinhoCap + 1)
	cases := []struct {
		name     string
		msg      *waE2E.Message
		viewOnce bool
		block    string
		mime     string
		caption  string
	}{
		{"imagem com legenda", &waE2E.Message{ImageMessage: &waE2E.ImageMessage{FileLength: big, Caption: proto.String("foto do pix"), JPEGThumbnail: []byte{1, 2, 3}}}, false, "imageMessage", "image/jpeg", "foto do pix"},
		{"video com legenda", &waE2E.Message{VideoMessage: &waE2E.VideoMessage{FileLength: big, Caption: proto.String("video do produto")}}, false, "videoMessage", "video/mp4", "video do produto"},
		{"audio ptt", &waE2E.Message{AudioMessage: &waE2E.AudioMessage{FileLength: big, PTT: proto.Bool(true)}}, false, "audioMessage", "audio/ogg", ""},
		{"documento com legenda", &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{FileLength: big, Mimetype: proto.String("application/zip"), FileName: proto.String("notas.zip"), Caption: proto.String("segue o zip")}}, false, "documentMessage", "application/zip", "segue o zip"},
		{"sticker", &waE2E.Message{StickerMessage: &waE2E.StickerMessage{FileLength: big}}, false, "stickerMessage", "image/png", ""},
		// view-once chega já desembrulhado pelo whatsmeow (UnwrapRaw, message.go:307).
		{"view-once imagem", &waE2E.Message{ImageMessage: &waE2E.ImageMessage{FileLength: big, ViewOnce: proto.Bool(true), Caption: proto.String("so uma vez")}}, true, "imageMessage", "image/jpeg", "so uma vez"},
		{"associated child video", &waE2E.Message{AssociatedChildMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{VideoMessage: &waE2E.VideoMessage{FileLength: big, Caption: proto.String("filho")}}}}, false, "associatedChildMessage", "video/mp4", "filho"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mycli, svc := espinhoClient(t, espinhoCap, false, true)
			evt := espinhoMsg("BIG-"+c.block, c.msg)
			evt.IsViewOnce = c.viewOnce
			if p := runHandler(t, mycli, evt); p != nil {
				t.Fatalf("tentou baixar mídia acima do teto (panic): %v", p)
			}
			msg := espinhoDataMessage(t, svc)
			if msg["mediaOmitted"] != true || msg["mediaOmittedReason"] != "size" {
				t.Fatalf("sem marcador: %v", msg)
			}
			if msg["mediaSize"] != float64(espinhoCap+1) || msg["mediaMaxBytes"] != float64(espinhoCap) {
				t.Fatalf("mediaSize/mediaMaxBytes: %v / %v", msg["mediaSize"], msg["mediaMaxBytes"])
			}
			if msg["mimetype"] != c.mime {
				t.Fatalf("mimetype = %v, esperado %s", msg["mimetype"], c.mime)
			}
			if _, has := msg["base64"]; has {
				t.Fatal("base64 no webhook acima do teto")
			}
			if _, has := msg[c.block]; !has {
				t.Fatalf("bloco %s sumiu do payload: %v", c.block, msg)
			}
			raw, _ := json.Marshal(msg)
			if c.caption != "" && !strings.Contains(string(raw), c.caption) {
				t.Fatalf("legenda perdida: %s", raw)
			}
		})
	}
}

// Fronteira: exatamente no teto ainda baixa; 1 byte acima não.
func TestEspinhoFronteiraDoTeto(t *testing.T) {
	mycli, svc := espinhoClient(t, espinhoCap, false, true)
	espinhoTentouBaixar(t, mycli, svc, espinhoMsg("AT-CAP", &waE2E.Message{VideoMessage: &waE2E.VideoMessage{FileLength: proto.Uint64(espinhoCap)}}))

	mycli2, svc2 := espinhoClient(t, espinhoCap, false, true)
	if p := runHandler(t, mycli2, espinhoMsg("CAP+1", &waE2E.Message{VideoMessage: &waE2E.VideoMessage{FileLength: proto.Uint64(espinhoCap + 1)}})); p != nil {
		t.Fatalf("panic: %v", p)
	}
	if espinhoDataMessage(t, svc2)["mediaOmitted"] != true || strings.Contains(instanceLog(t, mycli2), "Downloading video") {
		t.Fatal("1 byte acima do teto não deveria baixar")
	}
}

// espinhoTentouBaixar: o teto não cortou antes do download (o handler tentou baixar).
func espinhoTentouBaixar(t *testing.T, mycli *MyClient, svc *payloadService, evt *events.Message) {
	t.Helper()
	if p := runHandler(t, mycli, evt); p != nil {
		t.Fatalf("panic: %v", p)
	}
	msg := espinhoDataMessage(t, svc)
	if _, has := msg["mediaOmitted"]; has {
		t.Fatalf("teto cortou antes do download: %v", msg)
	}
	if !strings.Contains(instanceLog(t, mycli), "Failed to download media") {
		t.Fatal("handler não tentou baixar a mídia")
	}
}

// Sem FileLength, ou FileLength que estoura int64 (vira negativo): o corte prévio não
// age e o handler baixa; quem segura o webhook é a checagem pós-download (len(data)).
func TestEspinhoTamanhoDesconhecidoOuOverflowBaixaEChecaDepois(t *testing.T) {
	for name, fl := range map[string]*uint64{"sem FileLength": nil, "FileLength 2^64-1": proto.Uint64(^uint64(0))} {
		t.Run(name, func(t *testing.T) {
			mycli, svc := espinhoClient(t, espinhoCap, false, true)
			espinhoTentouBaixar(t, mycli, svc, espinhoMsg("UNK", &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{FileLength: fl, Mimetype: proto.String("application/pdf")}}))
		})
	}
}

// MinIO ligado: o teto não se aplica (mídia vai por URL), comportamento antigo.
func TestEspinhoMinioLigadoIgnoraTeto(t *testing.T) {
	mycli, svc := espinhoClient(t, espinhoCap, true, true)
	espinhoTentouBaixar(t, mycli, svc, espinhoMsg("MINIO", &waE2E.Message{VideoMessage: &waE2E.VideoMessage{FileLength: proto.Uint64(500 * 1024 * 1024)}}))
}

// Teto 0 desliga o corte: baixa como antes.
func TestEspinhoTetoZeroDesliga(t *testing.T) {
	mycli, svc := espinhoClient(t, 0, false, true)
	espinhoTentouBaixar(t, mycli, svc, espinhoMsg("NOCAP", &waE2E.Message{VideoMessage: &waE2E.VideoMessage{FileLength: proto.Uint64(500 * 1024 * 1024)}}))
}

// WEBHOOK_FILES=false: nada muda (sem download, sem marcador, legenda preservada).
func TestEspinhoWebhookFilesDesligadoSemMarcador(t *testing.T) {
	mycli, svc := espinhoClient(t, espinhoCap, false, false)
	evt := espinhoMsg("NOFILES", &waE2E.Message{VideoMessage: &waE2E.VideoMessage{FileLength: proto.Uint64(espinhoCap * 5), Caption: proto.String("legenda")}})
	if p := runHandler(t, mycli, evt); p != nil {
		t.Fatalf("panic: %v", p)
	}
	msg := espinhoDataMessage(t, svc)
	if _, has := msg["mediaOmitted"]; has {
		t.Fatalf("marcador sem WEBHOOK_FILES: %v", msg)
	}
	raw, _ := json.Marshal(msg)
	if !strings.Contains(string(raw), "legenda") {
		t.Fatalf("legenda perdida: %s", raw)
	}
}

// Regressão: texto puro e mensagem citada seguem iguais, sem campos novos.
func TestEspinhoTextoNormalSemCamposNovos(t *testing.T) {
	mycli, svc := espinhoClient(t, espinhoCap, false, true)
	evt := espinhoMsg("TXT", &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
		Text: proto.String("oi, tudo bem?"),
		ContextInfo: &waE2E.ContextInfo{
			StanzaID:      proto.String("Q1"),
			QuotedMessage: &waE2E.Message{VideoMessage: &waE2E.VideoMessage{FileLength: proto.Uint64(espinhoCap * 9)}},
		},
	}})
	evt.Info.Type = "text"
	if p := runHandler(t, mycli, evt); p != nil {
		t.Fatalf("panic: %v", p)
	}
	msg := espinhoDataMessage(t, svc)
	for _, k := range []string{"mediaOmitted", "mediaOmittedReason", "mediaSize", "mediaMaxBytes", "mimetype", "base64"} {
		if _, has := msg[k]; has {
			t.Fatalf("texto ganhou campo %s: %v", k, msg)
		}
	}
	raw, _ := json.Marshal(msg)
	if !strings.Contains(string(raw), "oi, tudo bem?") {
		t.Fatalf("texto perdido: %s", raw)
	}
}
