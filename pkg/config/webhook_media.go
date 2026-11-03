package config

import (
	"strconv"
	"strings"
)

// DefaultWebhookMaxMediaBytes é o teto padrão (10 MiB) da mídia embutida em
// base64 no webhook. Em base64 vira ~13.3 MiB, abaixo do limite de 16 MB de
// corpo do SBX.
const DefaultWebhookMaxMediaBytes int64 = 10 * 1024 * 1024

// parseWebhookMaxMediaBytes lê WEBHOOK_MAX_MEDIA_BYTES. Vazio ou inválido usa
// o padrão; 0 ou negativo desliga o teto.
func parseWebhookMaxMediaBytes(raw string) int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return DefaultWebhookMaxMediaBytes
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return DefaultWebhookMaxMediaBytes
	}
	return n
}
