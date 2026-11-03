package whatsmeow_service

import "go.mau.fi/whatsmeow/proto/waE2E"

// declaredMediaInfo devolve o tamanho declarado (FileLength) e o mimetype da
// mídia que o handler de Message vai baixar, na mesma ordem de prioridade do
// download: image, audio, document, video, sticker e depois as mesmas mídias
// do associatedChildMessage. size 0 = tamanho desconhecido.
func declaredMediaInfo(msg *waE2E.Message) (size int64, mimeType string) {
	if msg == nil {
		return 0, ""
	}
	pick := func(m *waE2E.Message) (int64, string, bool) {
		if m == nil {
			return 0, "", false
		}
		switch {
		case m.GetImageMessage() != nil:
			return int64(m.GetImageMessage().GetFileLength()), "image/jpeg", true
		case m.GetAudioMessage() != nil:
			return int64(m.GetAudioMessage().GetFileLength()), "audio/ogg", true
		case m.GetDocumentMessage() != nil:
			return int64(m.GetDocumentMessage().GetFileLength()), m.GetDocumentMessage().GetMimetype(), true
		case m.GetVideoMessage() != nil:
			return int64(m.GetVideoMessage().GetFileLength()), "video/mp4", true
		case m.GetStickerMessage() != nil:
			return int64(m.GetStickerMessage().GetFileLength()), "image/png", true
		}
		return 0, "", false
	}
	if s, mt, ok := pick(msg); ok {
		return s, mt
	}
	if child := msg.GetAssociatedChildMessage(); child != nil {
		if s, mt, ok := pick(child.GetMessage()); ok {
			return s, mt
		}
	}
	return 0, ""
}

// mediaExceedsCap: true quando há teto (>0) e o tamanho passa dele.
func mediaExceedsCap(size, maxBytes int64) bool {
	return maxBytes > 0 && size > maxBytes
}

// markMediaOmitted sinaliza no payload que a mídia não foi embutida por
// tamanho; o consumidor usa /message/downloadmedia ou mostra placeholder.
func markMediaOmitted(messageMap map[string]interface{}, size, maxBytes int64, mimeType string) {
	messageMap["mediaOmitted"] = true
	messageMap["mediaOmittedReason"] = "size"
	messageMap["mediaSize"] = size
	messageMap["mediaMaxBytes"] = maxBytes
	if mimeType != "" {
		messageMap["mimetype"] = mimeType
	}
}
