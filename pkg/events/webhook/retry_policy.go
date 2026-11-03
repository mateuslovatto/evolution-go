package webhook_producer

// isRetryableStatus diz se vale reenviar um webhook que voltou com statusCode.
// 0 = falha de rede/transporte (retry). Só erros definitivos do pedido não
// reenviam: 400, 401, 403, 413 e 422. O resto (404 de proxy no deploy, 408,
// 429, 5xx) reenvia.
func isRetryableStatus(statusCode int) bool {
	switch statusCode {
	case 400, 401, 403, 413, 422:
		return false
	}
	return true
}
