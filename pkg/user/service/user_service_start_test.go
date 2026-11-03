package user_service

import (
	"errors"
	"testing"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	whatsmeow_service "github.com/evolution-foundation/evolution-go/pkg/whatsmeow/service"
	"go.mau.fi/whatsmeow"
)

type startSpy struct {
	whatsmeow_service.WhatsmeowService
	starts int
}

func (s *startSpy) StartInstance(string) error {
	s.starts++
	return errors.New("spy: nao inicia")
}

// GET avatar do SBX: sem cliente em memória devolve ErrInstanceNotConnected
// (409) e NUNCA chama StartInstance.
func TestGetAvatarNaoIniciaCliente(t *testing.T) {
	spy := &startSpy{}
	svc := &userService{
		clientPointer:    map[string]*whatsmeow.Client{},
		whatsmeowService: spy,
	}

	_, err := svc.GetAvatar(&GetAvatarStruct{Number: "5511999999999"}, &instance_model.Instance{Id: "inst-sem-cliente"})

	if !errors.Is(err, whatsmeow_service.ErrInstanceNotConnected) {
		t.Fatalf("esperado ErrInstanceNotConnected, veio %v", err)
	}
	if spy.starts != 0 {
		t.Errorf("GetAvatar chamou StartInstance %d vez(es)", spy.starts)
	}
}
