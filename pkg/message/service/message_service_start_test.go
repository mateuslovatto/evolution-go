package message_service

import (
	"errors"
	"testing"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	whatsmeow_service "github.com/evolution-foundation/evolution-go/pkg/whatsmeow/service"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
)

type startSpy struct {
	whatsmeow_service.WhatsmeowService
	starts int
}

func (s *startSpy) StartInstance(string) error {
	s.starts++
	return errors.New("spy: nao inicia")
}

// Rotas que o SBX chama em leitura: sem cliente em memória devolvem
// ErrInstanceNotConnected (409) e NUNCA chamam StartInstance.
func TestReadRoutesNaoIniciamCliente(t *testing.T) {
	inst := &instance_model.Instance{Id: "inst-sem-cliente"}
	msg := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{}}

	cases := map[string]func(*messageService) error{
		"downloadmedia": func(s *messageService) error {
			_, _, err := s.DownloadMedia(&DownloadMediaStruct{Message: msg}, inst, nil)
			return err
		},
		"presence": func(s *messageService) error {
			_, err := s.ChatPresence(&ChatPresenceStruct{Number: "5511999999999", State: "composing"}, inst)
			return err
		},
	}

	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			spy := &startSpy{}
			svc := &messageService{
				clientPointer:    map[string]*whatsmeow.Client{},
				whatsmeowService: spy,
			}

			err := call(svc)

			if !errors.Is(err, whatsmeow_service.ErrInstanceNotConnected) {
				t.Fatalf("esperado ErrInstanceNotConnected, veio %v", err)
			}
			if spy.starts != 0 {
				t.Errorf("rota chamou StartInstance %d vez(es)", spy.starts)
			}
		})
	}
}
