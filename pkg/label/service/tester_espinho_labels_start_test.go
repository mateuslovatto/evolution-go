package label_service

// X1 do QA (sbx-etiqueta): GET /label/list chamava StartInstance (vetor do
// incidente de 29/10). GetLabels agora só lê o banco; as escritas usam a
// checagem sem start do D3 e devolvem ErrLabelSyncNotConnected.

import (
	"errors"
	"testing"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	"go.mau.fi/whatsmeow"
)

func TestEspinhoGetLabelsNaoIniciaCliente(t *testing.T) {
	h := newSyncHarness(t)
	spy := &startSpy{}
	h.svc.clientPointer = map[string]*whatsmeow.Client{}
	h.svc.whatsmeowService = spy

	_, _ = h.svc.GetLabels(&instance_model.Instance{Id: "inst-sem-cliente"})

	if spy.starts != 0 {
		t.Errorf("GET /label/list chamou StartInstance %d vez(es)", spy.starts)
	}
}

func TestGetLabelsLeDoBancoSemCliente(t *testing.T) {
	h := newSyncHarness(t)
	inst := &instance_model.Instance{Id: "inst-a"}
	seedLabels(h, inst.Id, 2)

	labels, err := h.svc.GetLabels(inst)
	if err != nil {
		t.Fatalf("GetLabels deveria ler o banco sem cliente: %v", err)
	}
	if len(labels) != 2 {
		t.Fatalf("GetLabels devolveu %d etiquetas, esperado 2", len(labels))
	}
}

func TestEscritasDeEtiquetaNaoIniciamCliente(t *testing.T) {
	inst := &instance_model.Instance{Id: "inst-sem-cliente"}
	cases := map[string]func(*labelService) error{
		"chat": func(s *labelService) error {
			return s.ChatLabel(&ChatLabelStruct{JID: "5511999999999@s.whatsapp.net", LabelID: "1"}, inst)
		},
		"unchat": func(s *labelService) error {
			return s.ChatUnlabel(&ChatLabelStruct{JID: "5511999999999@s.whatsapp.net", LabelID: "1"}, inst)
		},
		"message": func(s *labelService) error {
			return s.MessageLabel(&MessageLabelStruct{JID: "5511999999999@s.whatsapp.net", LabelID: "1", MessageID: "msg"}, inst)
		},
		"unmessage": func(s *labelService) error {
			return s.MessageUnlabel(&MessageLabelStruct{JID: "5511999999999@s.whatsapp.net", LabelID: "1", MessageID: "msg"}, inst)
		},
		"edit": func(s *labelService) error {
			return s.EditLabel(&EditLabelStruct{LabelID: "1", Name: "Pago"}, inst)
		},
	}

	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			h := newSyncHarness(t)
			spy := &startSpy{}
			h.svc.clientPointer = map[string]*whatsmeow.Client{}
			h.svc.whatsmeowService = spy

			err := call(h.svc)

			if !errors.Is(err, ErrLabelSyncNotConnected) {
				t.Fatalf("esperado ErrLabelSyncNotConnected, veio %v", err)
			}
			if spy.starts != 0 {
				t.Errorf("escrita de etiqueta chamou StartInstance %d vez(es)", spy.starts)
			}
		})
	}
}
