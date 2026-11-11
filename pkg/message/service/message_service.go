package message_service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	message_model "github.com/evolution-foundation/evolution-go/pkg/message/model"
	message_repository "github.com/evolution-foundation/evolution-go/pkg/message/repository"
	"github.com/evolution-foundation/evolution-go/pkg/utils"
	whatsmeow_service "github.com/evolution-foundation/evolution-go/pkg/whatsmeow/service"
	"github.com/vincent-petithory/dataurl"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

type MessageService interface {
	React(data *ReactStruct, instance *instance_model.Instance) (*MessageSendStruct, error)
	ChatPresence(data *ChatPresenceStruct, instance *instance_model.Instance) (string, error)
	MarkRead(data *MarkReadStruct, instance *instance_model.Instance) (string, error)
	MarkPlayed(data *MarkPlayedStruct, instance *instance_model.Instance) (string, error)
	DownloadMedia(data *DownloadMediaStruct, instance *instance_model.Instance, request *http.Request) (*dataurl.DataURL, string, error)
	GetMessageStatus(data *MessageStatusStruct, instance *instance_model.Instance) (*message_model.Message, string, error)
	DeleteMessageEveryone(data *MessageStruct, instance *instance_model.Instance) (string, string, error)
	EditMessage(data *EditMessageStruct, instance *instance_model.Instance) (string, string, error)
}

type messageService struct {
	clientPointer     map[string]*whatsmeow.Client
	messageRepository message_repository.MessageRepository
	whatsmeowService  whatsmeow_service.WhatsmeowService
	loggerWrapper     *logger_wrapper.LoggerManager
}

type ReactStruct struct {
	Number      string `json:"number"`
	Reaction    string `json:"reaction"`
	Id          string `json:"id"`
	FromMe      bool   `json:"fromMe"`
	Participant string `json:"participant,omitempty"`
}

type ChatPresenceStruct struct {
	Number  string `json:"number"`
	State   string `json:"state"`
	IsAudio bool   `json:"isAudio"`
	// Delay, in milliseconds, keeps the "composing"/"recording" indicator alive
	// for the given duration (re-sending it periodically) and then sends "paused".
	// Only applies when State is "composing". 0 = single fire (legacy behaviour).
	Delay int `json:"delay"`
}

type MarkReadStruct struct {
	Id     []string `json:"id"`
	Number string   `json:"number"`
}

type MarkPlayedStruct struct {
	Id     []string `json:"id"`
	Number string   `json:"number"`
}

type DownloadMediaStruct struct {
	Message *waE2E.Message `json:"message"`
}

type MessageStatusStruct struct {
	Id string `json:"id"`
}

type MessageStruct struct {
	Chat      string `json:"chat"`
	MessageID string `json:"messageId"`
}

type EditMessageStruct struct {
	Chat      string `json:"chat"`
	Message   string `json:"message"`
	MessageID string `json:"messageId"`
}

type MessageSendStruct struct {
	Info               types.MessageInfo
	Message            *waE2E.Message
	MessageContextInfo *waE2E.ContextInfo
}

func (m *messageService) ensureClientConnected(instanceId string) (*whatsmeow.Client, error) {
	client := m.clientPointer[instanceId]
	m.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Checking client connection status - Client exists: %v", instanceId, client != nil)

	if client == nil {
		m.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] No client found, attempting to start new instance", instanceId)
		err := m.whatsmeowService.StartInstance(instanceId)
		if err != nil {
			m.loggerWrapper.GetLogger(instanceId).LogError("[%s] Failed to start instance: %v", instanceId, err)
			return nil, errors.New("no active session found")
		}

		m.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Instance started, waiting 2 seconds...", instanceId)
		time.Sleep(2 * time.Second)

		client = m.clientPointer[instanceId]
		m.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Checking new client - Exists: %v, Connected: %v",
			instanceId,
			client != nil,
			client != nil && client.IsConnected())

		if client == nil || !client.IsConnected() {
			m.loggerWrapper.GetLogger(instanceId).LogError("[%s] New client validation failed - Exists: %v, Connected: %v",
				instanceId,
				client != nil,
				client != nil && client.IsConnected())
			return nil, errors.New("no active session found")
		}
	} else if !client.IsConnected() {
		m.loggerWrapper.GetLogger(instanceId).LogError("[%s] Existing client is disconnected - Connected status: %v",
			instanceId,
			client.IsConnected())
		return nil, errors.New("client disconnected")
	}

	m.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Client successfully validated - Connected: %v", instanceId, client.IsConnected())
	return client, nil
}

func (m *messageService) React(data *ReactStruct, instance *instance_model.Instance) (*MessageSendStruct, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return nil, err
	}

	msgId := ""

	recipient, ok := utils.ParseJID(data.Number)
	if !ok {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return nil, errors.New("invalid phone number")
	}

	// Strip the "+" that ParseJID/CreateJID adds. The recipient is used both as
	// the SendMessage target (usync/device resolution) AND as the MessageKey
	// RemoteJID that references the reacted message's chat. A malformed "+JID"
	// breaks device resolution (usync) and prevents the reaction from matching
	// the original message's chat. See utils.CanonicalJID.
	recipient = utils.CanonicalJID(recipient)

	if data.Id == "" {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Missing Id in Payload", instance.Id)
		return nil, errors.New("missing id in payload")
	} else {
		msgId = data.Id
	}

	fromMe := data.FromMe
	reaction := data.Reaction
	if reaction == "remove" {
		reaction = ""
	}

	// Create MessageKey — msgId is the ID of the message being reacted to,
	// NOT the ID of the reaction envelope itself.
	messageKey := &waCommon.MessageKey{
		RemoteJID: proto.String(recipient.String()),
		FromMe:    proto.Bool(fromMe),
		ID:        proto.String(msgId),
	}

	// Add participant if provided (for group messages)
	if data.Participant != "" {
		participantJID, ok := utils.ParseJID(data.Participant)
		if ok {
			messageKey.Participant = proto.String(utils.CanonicalJID(participantJID).String())
		}
	}

	msg := &waE2E.Message{
		ReactionMessage: &waE2E.ReactionMessage{
			Key:               messageKey,
			Text:              proto.String(reaction),
			SenderTimestampMS: proto.Int64(time.Now().UnixMilli()),
		},
	}

	// Do NOT pass ID: msgId in SendRequestExtra. Doing so would reuse the
	// original message ID as the reaction envelope ID; WhatsApp silently
	// deduplicates it and drops the reaction. Let whatsmeow generate a
	// fresh, unique ID for the envelope.
	response, err := client.SendMessage(context.Background(), recipient, msg)
	if err != nil {
		return nil, err
	}

	isGroup := strings.Contains(data.Number, "@g.us")
	messageType := "ReactionMessage"

	messageInfo := types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     recipient,
			Sender:   *client.Store.ID,
			IsFromMe: true,
			IsGroup:  isGroup,
		},
		ID:        response.ID,
		Timestamp: time.Now(),
		ServerID:  response.ServerID,
		Type:      messageType,
	}

	messageSent := &MessageSendStruct{
		Info:    messageInfo,
		Message: msg,
	}

	return messageSent, nil
}

func (m *messageService) ChatPresence(data *ChatPresenceStruct, instance *instance_model.Instance) (string, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return "", err
	}

	var ts time.Time

	recipient, ok := utils.ParseJID(data.Number)
	if !ok {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return "", errors.New("invalid phone number")
	}

	// chatstate (typing) is a RAW node sent without usync normalization, so it
	// needs a canonical digits-only JID or WhatsApp silently drops it. See
	// utils.CanonicalJID for the full rationale.
	recipient = utils.CanonicalJID(recipient)

	media := ""

	if data.IsAudio {
		media = "audio"
	}

	// WhatsApp only forwards chatstate (typing / recording) events to the
	// recipient while the sender is marked online. SendChatPresence merely
	// sends the chatstate node — it does NOT mark us available. Background
	// presence handling (events.AppStateSyncComplete) may have set us to
	// Unavailable, in which case the server silently drops the typing
	// indicator. Mark ourselves available first to guarantee delivery.
	//
	// When alwaysOnline is false the device must not stay available (WhatsApp
	// would then suppress push notifications on the user's phone), so a
	// debounced restore to Unavailable is scheduled once the chatstate is done
	// (see scheduleUnavailableRestore below). Cancel any pending restore now so
	// it cannot fire in the middle of this indicator.
	if !instance.AlwaysOnline {
		cancelUnavailableRestore(instance.Id)
	}
	if presErr := client.SendPresence(context.Background(), types.PresenceAvailable); presErr != nil {
		m.loggerWrapper.GetLogger(instance.Id).LogWarn("[%s] SendPresence(available) before chatstate failed (non-fatal): %v", instance.Id, presErr)
	}

	state := types.ChatPresence(data.State)
	mediaType := types.ChatPresenceMedia(media)

	// Composing/recording without a Delay is a single fire: keep the device
	// available long enough for the indicator to show before restoring
	// Unavailable. In every other case the indicator is finished (paused, or
	// the Delay loop below sends paused) and a short grace period is enough.
	restoreAfter := presenceRestoreShortGrace
	if data.Delay <= 0 && state != types.ChatPresencePaused {
		restoreAfter = presenceRestoreIndicatorGrace
	}
	if !instance.AlwaysOnline {
		defer m.scheduleUnavailableRestore(client, instance.Id, restoreAfter)
	}

	err = client.SendChatPresence(context.Background(), recipient, state, mediaType)
	if err != nil {
		return "", err
	}

	// A single "composing" indicator is ephemeral: WhatsApp expires it after a
	// few seconds unless refreshed. When a Delay is provided (and we're typing),
	// keep the indicator alive for the requested duration by re-sending it, then
	// send "paused" so the indicator clears cleanly instead of timing out.
	if data.Delay > 0 && state == types.ChatPresenceComposing {
		const keepAliveInterval = 5 * time.Second
		const maxDelay = 60 * time.Second

		remaining := time.Duration(data.Delay) * time.Millisecond
		if remaining > maxDelay {
			remaining = maxDelay
		}

		for remaining > 0 {
			sleep := keepAliveInterval
			if remaining < sleep {
				sleep = remaining
			}
			time.Sleep(sleep)
			remaining -= sleep

			if remaining > 0 {
				// Refresh the indicator so it doesn't expire mid-delay.
				if refreshErr := client.SendChatPresence(context.Background(), recipient, state, mediaType); refreshErr != nil {
					m.loggerWrapper.GetLogger(instance.Id).LogWarn("[%s] Refresh chatstate failed (non-fatal): %v", instance.Id, refreshErr)
				}
			}
		}

		if pausedErr := client.SendChatPresence(context.Background(), recipient, types.ChatPresencePaused, mediaType); pausedErr != nil {
			m.loggerWrapper.GetLogger(instance.Id).LogWarn("[%s] SendChatPresence(paused) failed (non-fatal): %v", instance.Id, pausedErr)
		}
	}

	m.loggerWrapper.GetLogger(instance.Id).LogInfo("Presence (%s) sent to %s", data.State, data.Number)

	return ts.String(), nil
}

const (
	// presenceRestoreShortGrace is used once the chatstate has been cleared
	// (paused sent); presenceRestoreIndicatorGrace keeps a single-fire
	// composing/recording indicator visible before going Unavailable.
	presenceRestoreShortGrace     = 3 * time.Second
	presenceRestoreIndicatorGrace = 20 * time.Second
)

// pendingUnavailable holds one debounced "restore Unavailable" timer per
// instance, so consecutive /message/presence calls keep the device available
// only while they last.
var pendingUnavailable = struct {
	sync.Mutex
	timers map[string]*time.Timer
}{timers: make(map[string]*time.Timer)}

func cancelUnavailableRestore(instanceID string) {
	pendingUnavailable.Lock()
	defer pendingUnavailable.Unlock()
	if t, ok := pendingUnavailable.timers[instanceID]; ok {
		t.Stop()
		delete(pendingUnavailable.timers, instanceID)
	}
}

// scheduleUnavailableRestore marks the device Unavailable again after d,
// unless another presence call reschedules it first. Used for instances with
// alwaysOnline=false, which must not remain permanently available.
func (m *messageService) scheduleUnavailableRestore(client *whatsmeow.Client, instanceID string, d time.Duration) {
	pendingUnavailable.Lock()
	defer pendingUnavailable.Unlock()
	if t, ok := pendingUnavailable.timers[instanceID]; ok {
		t.Stop()
	}
	var timer *time.Timer
	timer = time.AfterFunc(d, func() {
		pendingUnavailable.Lock()
		if pendingUnavailable.timers[instanceID] != timer {
			pendingUnavailable.Unlock()
			return // superseded by a newer presence call
		}
		delete(pendingUnavailable.timers, instanceID)
		pendingUnavailable.Unlock()

		if client == nil || !client.IsConnected() {
			return
		}
		if err := client.SendPresence(context.Background(), types.PresenceUnavailable); err != nil {
			m.loggerWrapper.GetLogger(instanceID).LogWarn("[%s] Restoring unavailable presence after chatstate failed (non-fatal): %v", instanceID, err)
		}
	})
	pendingUnavailable.timers[instanceID] = timer
}

func (m *messageService) MarkRead(data *MarkReadStruct, instance *instance_model.Instance) (string, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return "", err
	}

	var ts time.Time

	jid, ok := utils.ParseJID(data.Number)
	if !ok {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return "", errors.New("invalid phone number")
	}

	// Read receipts are RAW nodes (no usync) — strip the "+" so the receipt
	// reaches the recipient. Same root cause as the typing fix above.
	jid = utils.CanonicalJID(jid)

	err = client.MarkRead(context.Background(), data.Id, time.Now(), jid, jid)
	if err != nil {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error marking message as read: %v", instance.Id, err)
		return "", errors.New("error marking message as read")
	}

	return ts.String(), nil
}

func (m *messageService) MarkPlayed(data *MarkPlayedStruct, instance *instance_model.Instance) (string, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return "", err
	}

	var ts time.Time

	jid, ok := utils.ParseJID(data.Number)
	if !ok {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return "", errors.New("invalid phone number")
	}

	// Played receipts are RAW nodes (no usync) — strip the "+" so the receipt
	// reaches the recipient. Same root cause as the MarkRead fix.
	jid = utils.CanonicalJID(jid)

	err = client.MarkRead(context.Background(), data.Id, time.Now(), jid, jid, types.ReceiptTypePlayed)
	if err != nil {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error marking message as played: %v", instance.Id, err)
		return "", errors.New("error marking message as played")
	}

	return ts.String(), nil
}

func (m *messageService) DownloadMedia(data *DownloadMediaStruct, instance *instance_model.Instance, request *http.Request) (*dataurl.DataURL, string, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return nil, "", err
	}

	var ts time.Time

	msg := data.Message

	mimetype := ""
	var mediaData []byte

	img := msg.GetImageMessage()
	audio := msg.GetAudioMessage()
	document := msg.GetDocumentMessage()
	video := msg.GetVideoMessage()
	sticker := msg.GetStickerMessage()

	if img == nil && audio == nil && document == nil && video == nil && sticker == nil {
		return nil, "", errors.New("invalid media type")
	}

	userDirectory := fmt.Sprintf(`files/user_%s`, instance.Id)
	_, err = os.Stat(userDirectory)
	if os.IsNotExist(err) {
		errDir := os.MkdirAll(userDirectory, 0751)
		if errDir != nil {
			m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Could not create user directory (%s)", instance.Id, userDirectory)
			return nil, "", errDir
		}
	}

	if img != nil {
		mediaData, err = client.Download(context.Background(), img)
		if err != nil {
			m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Failed to download image", instance.Id)
			msg := fmt.Sprintf("Failed to download image %v", err)
			return nil, "", errors.New(msg)
		}
		mimetype = img.GetMimetype()
	}

	if audio != nil {
		mediaData, err = client.Download(context.Background(), audio)
		if err != nil {
			m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Failed to download audio", instance.Id)
			msg := fmt.Sprintf("Failed to download audio %v", err)
			return nil, "", errors.New(msg)
		}
		mimetype = audio.GetMimetype()
	}

	if document != nil {
		mediaData, err = client.Download(context.Background(), document)
		if err != nil {
			m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Failed to download document", instance.Id)
			msg := fmt.Sprintf("Failed to download document %v", err)
			return nil, "", errors.New(msg)
		}
		mimetype = document.GetMimetype()
	}

	if video != nil {
		mediaData, err = client.Download(context.Background(), video)
		if err != nil {
			m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Failed to download video", instance.Id)
			msg := fmt.Sprintf("Failed to download video %v", err)
			return nil, "", errors.New(msg)
		}
		mimetype = video.GetMimetype()
	}

	if sticker != nil {
		mediaData, err = client.Download(context.Background(), sticker)
		if err != nil {
			m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Failed to download sticker", instance.Id)
			msg := fmt.Sprintf("Failed to download sticker %v", err)
			return nil, "", errors.New(msg)
		}
		mimetype = sticker.GetMimetype()
	}

	dataURL := dataurl.New(mediaData, mimetype)

	return dataURL, ts.String(), nil
}

func (m *messageService) GetMessageStatus(data *MessageStatusStruct, instance *instance_model.Instance) (*message_model.Message, string, error) {
	_, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return nil, "", err
	}

	var ts time.Time

	result, err := m.messageRepository.GetMessageByID(data.Id)
	if err != nil {
		return nil, "", err
	}

	return result, ts.String(), nil
}

func (m *messageService) DeleteMessageEveryone(data *MessageStruct, instance *instance_model.Instance) (string, string, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return "", "", err
	}

	var ts time.Time

	recipient, ok := utils.ParseJID(data.Chat)
	if !ok {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return "", "", errors.New("invalid phone number")
	}

	m.loggerWrapper.GetLogger(instance.Id).LogInfo("Revoking message %s from %s", data.MessageID, recipient)

	resp, err := client.SendMessage(
		context.Background(),
		recipient,
		client.BuildRevoke(recipient, types.EmptyJID, data.MessageID))
	if err != nil {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error revoking message: %v", instance.Id, err)
		return "", "", err
	}

	response := resp.ID

	return response, ts.String(), nil
}

func (m *messageService) EditMessage(data *EditMessageStruct, instance *instance_model.Instance) (string, string, error) {
	client, err := m.ensureClientConnected(instance.Id)
	if err != nil {
		return "", "", err
	}

	recipient, ok := utils.ParseJID(data.Chat)
	if !ok {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Error validating message fields", instance.Id)
		return "", "", errors.New("invalid phone number")
	}

	resp, err := client.SendMessage(
		context.Background(),
		recipient,
		client.BuildEdit(
			recipient,
			data.MessageID,
			&waE2E.Message{
				ExtendedTextMessage: &waE2E.ExtendedTextMessage{
					Text: &data.Message,
				},
			}))
	if err != nil {
		m.loggerWrapper.GetLogger(instance.Id).LogError("[%s] error revoking message: %v", instance.Id, err)
		return "", "", err
	}

	return resp.ID, resp.Timestamp.String(), nil
}

func NewMessageService(
	clientPointer map[string]*whatsmeow.Client,
	messageRepository message_repository.MessageRepository,
	whatsmeowService whatsmeow_service.WhatsmeowService,
	loggerWrapper *logger_wrapper.LoggerManager,
) MessageService {
	return &messageService{
		clientPointer:     clientPointer,
		messageRepository: messageRepository,
		whatsmeowService:  whatsmeowService,
		loggerWrapper:     loggerWrapper,
	}
}
