package whatsmeow_service

import (
	"errors"

	"go.mau.fi/whatsmeow"
)

// ErrInstanceNotConnected é devolvido por rotas que não podem subir cliente:
// a instância precisa já ter um cliente conectado e pareado em memória.
var ErrInstanceNotConnected = errors.New("instancia nao conectada")

// ConnectedClient devolve o cliente já em memória sem chamar StartInstance.
// StartInstance abriria uma sessão (QR) para instância sem pareamento — o vetor
// do incidente de 29/10. Rotas de leitura do SBX usam esta checagem.
func ConnectedClient(clientPointer map[string]*whatsmeow.Client, instanceID string) (*whatsmeow.Client, error) {
	client := clientPointer[instanceID]
	if client == nil || !client.IsConnected() || !client.IsLoggedIn() {
		return nil, ErrInstanceNotConnected
	}
	return client, nil
}
