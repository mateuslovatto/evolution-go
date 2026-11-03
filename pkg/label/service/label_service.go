package label_service

import (
	"context"
	"errors"
	"time"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	label_model "github.com/evolution-foundation/evolution-go/pkg/label/model"
	label_repository "github.com/evolution-foundation/evolution-go/pkg/label/repository"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/evolution-foundation/evolution-go/pkg/utils"
	whatsmeow_service "github.com/evolution-foundation/evolution-go/pkg/whatsmeow/service"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
)

type LabelService interface {
	ChatLabel(data *ChatLabelStruct, instance *instance_model.Instance) error
	MessageLabel(data *MessageLabelStruct, instance *instance_model.Instance) error
	EditLabel(data *EditLabelStruct, instance *instance_model.Instance) error
	ChatUnlabel(data *ChatLabelStruct, instance *instance_model.Instance) error
	MessageUnlabel(data *MessageLabelStruct, instance *instance_model.Instance) error
	GetLabels(instance *instance_model.Instance) ([]label_model.Label, error)
	SyncLabels(instance *instance_model.Instance) error
}

type labelService struct {
	clientPointer    map[string]*whatsmeow.Client
	whatsmeowService whatsmeow_service.WhatsmeowService
	labelRepository  label_repository.LabelRepository
	loggerWrapper    *logger_wrapper.LoggerManager

	// /label/sync collaborators, replaceable in tests.
	syncCooldown    *labelSyncCooldownTracker
	connectionCheck func(instanceID string) error
	fullSync        func(ctx context.Context, instanceID string) error
	clock           func() time.Time // nil = time.Now
}

type ChatLabelStruct struct {
	JID     string `json:"jid"`
	LabelID string `json:"labelId"`
}

type MessageLabelStruct struct {
	JID       string `json:"jid"`
	LabelID   string `json:"labelId"`
	MessageID string `json:"messageId"`
}

type EditLabelStruct struct {
	LabelID string `json:"labelId"`
	Name    string `json:"name"`
	Color   int    `json:"color"`
	Deleted bool   `json:"deleted"`
}

// connectedClient returns the client already in memory. It never calls
// StartInstance: starting a client for an unpaired instance opens a QR session
// (the vector of the 29/10 incident).
func (l *labelService) connectedClient(instanceId string) (*whatsmeow.Client, error) {
	if err := l.checkConnected(instanceId); err != nil {
		return nil, err
	}
	return l.clientPointer[instanceId], nil
}

func (l *labelService) ChatLabel(data *ChatLabelStruct, instance *instance_model.Instance) error {
	client, err := l.connectedClient(instance.Id)
	if err != nil {
		return err
	}

	jid, ok := utils.ParseJID(data.JID)
	if !ok {
		l.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error parse community jid", instance.Id)
		return errors.New("error parse community jid")
	}

	err = client.SendAppState(context.Background(), appstate.BuildLabelChat(
		jid,
		data.LabelID,
		true,
	))
	if err != nil {
		l.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error label chat: %v", instance.Id, err)
		return err
	}

	return nil
}

func (l *labelService) MessageLabel(data *MessageLabelStruct, instance *instance_model.Instance) error {
	client, err := l.connectedClient(instance.Id)
	if err != nil {
		return err
	}

	jid, ok := utils.ParseJID(data.JID)
	if !ok {
		l.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error parse community jid", instance.Id)
		return errors.New("error parse community jid")
	}

	err = client.SendAppState(context.Background(), appstate.BuildLabelMessage(
		jid,
		data.LabelID,
		data.MessageID,
		true,
	))
	if err != nil {
		l.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error label message: %v", instance.Id, err)
		return err
	}

	return nil
}

func (l *labelService) EditLabel(data *EditLabelStruct, instance *instance_model.Instance) error {
	client, err := l.connectedClient(instance.Id)
	if err != nil {
		return err
	}

	err = client.SendAppState(context.Background(), appstate.BuildLabelEdit(
		data.LabelID,
		data.Name,
		int32(data.Color),
		data.Deleted,
	))
	if err != nil {
		l.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error label message: %v", instance.Id, err)
		return err
	}

	return nil
}

func (l *labelService) ChatUnlabel(data *ChatLabelStruct, instance *instance_model.Instance) error {
	client, err := l.connectedClient(instance.Id)
	if err != nil {
		return err
	}

	jid, ok := utils.ParseJID(data.JID)
	if !ok {
		l.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error parse community jid", instance.Id)
		return errors.New("error parse community jid")
	}

	err = client.SendAppState(context.Background(), appstate.BuildLabelChat(
		jid,
		data.LabelID,
		false,
	))
	if err != nil {
		l.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error label chat: %v", instance.Id, err)
		return err
	}

	return nil
}

func (l *labelService) MessageUnlabel(data *MessageLabelStruct, instance *instance_model.Instance) error {
	client, err := l.connectedClient(instance.Id)
	if err != nil {
		return err
	}

	jid, ok := utils.ParseJID(data.JID)
	if !ok {
		l.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error parse community jid", instance.Id)
		return errors.New("error parse community jid")
	}

	err = client.SendAppState(context.Background(), appstate.BuildLabelMessage(
		jid,
		data.LabelID,
		data.MessageID,
		false,
	))
	if err != nil {
		l.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error label message: %v", instance.Id, err)
		return err
	}

	return nil
}

// GetLabels apenas lê o banco: as etiquetas já foram gravadas pelo full sync e
// pelo LabelEdit. Nunca sobe cliente (GET /label/list é chamado em toda listagem).
func (l *labelService) GetLabels(instance *instance_model.Instance) ([]label_model.Label, error) {
	labels, err := l.labelRepository.GetAllLabelsByInstanceID(instance.Id)
	if err != nil {
		l.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error fetching labels from database: %v", instance.Id, err)
		return nil, err
	}

	return labels, nil
}

func NewLabelService(
	clientPointer map[string]*whatsmeow.Client,
	whatsmeowService whatsmeow_service.WhatsmeowService,
	labelRepository label_repository.LabelRepository,
	loggerWrapper *logger_wrapper.LoggerManager,
) LabelService {
	svc := &labelService{
		clientPointer:    clientPointer,
		whatsmeowService: whatsmeowService,
		labelRepository:  labelRepository,
		loggerWrapper:    loggerWrapper,
		syncCooldown:     newLabelSyncCooldownTracker(labelSyncCooldown),
	}
	svc.connectionCheck = svc.checkConnected
	svc.fullSync = svc.fetchRegularAppState
	return svc
}
