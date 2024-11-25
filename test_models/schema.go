package test_models

import (
	"encoding/json"
	"time"

	"github.com/IBM/sarama"
	"github.com/leavez/go-optional"
)

type DeliveryEventType int8
type BidEventType int8
type TxStateUpdateType int8

const (
	// InputEvent types
	BidResultEvent BidEventType = 0
	PlaybackEvent  BidEventType = 1

	// DeliveryEvent types
	None            DeliveryEventType = 0
	ImpressionEvent DeliveryEventType = 1
	CompletionEvent DeliveryEventType = 2
)

type BidEvent struct {
	CampaignID string       `json:"cmpid"`
	Quart      int8         `json:"quart"`
	EventType  BidEventType `json:"event_type"`
}

type DeliveryEvent struct {
	CampaignID string            `json:"campaign_id"`
	EventType  DeliveryEventType `json:"event_type"`
}

type BidEventMessage struct {
	TxID  string
	Event BidEvent
}

func (e *BidEventMessage) EncodeMessage(topicPrefix string) *sarama.ProducerMessage {
	b, _ := json.Marshal(e.Event)
	return &sarama.ProducerMessage{
		Topic: topicPrefix,
		Key:   sarama.StringEncoder(e.TxID),
		Value: sarama.ByteEncoder(b),
	}
}

func (e *BidEventMessage) DecodeMessage(message *sarama.ConsumerMessage) error {
	err := json.Unmarshal(message.Value, &e.Event)
	if err != nil {
		return err
	}
	e.TxID = string(message.Key)
	return nil
}

type DeliveryEventMessage struct {
	TxID  string
	Event DeliveryEvent
}

func (e *DeliveryEventMessage) EncodeMessage(topicPrefix string) *sarama.ProducerMessage {
	b, _ := json.Marshal(e)
	return &sarama.ProducerMessage{
		Topic: topicPrefix,
		Key:   sarama.StringEncoder(e.TxID),
		Value: sarama.ByteEncoder(b),
	}
}

func (e *DeliveryEventMessage) DecodeMessage(message *sarama.ConsumerMessage) error {
	err := json.Unmarshal(message.Value, &e)
	if err != nil {
		return err
	}
	e.TxID = string(message.Key)
	return nil
}

type TxStateMessage struct {
	TxID  string
	State optional.Type[TxState]
}

type TxState struct {
	CampaignID string    `json:"campaignID"`
	Quart      int8      `json:"quart"`
	BidTime    time.Time `json:"bidTime"`
}

func (m *TxStateMessage) EncodeMessage(topicPrefix string) *sarama.ProducerMessage {
	var v sarama.Encoder
	if m.State.IsNil() {
		v = nil
	} else {
		b, _ := json.Marshal(m.State.ForceValue())
		v = sarama.ByteEncoder(b)
	}
	return &sarama.ProducerMessage{
		Topic: topicPrefix,
		Key:   sarama.StringEncoder(m.TxID),
		Value: v,
	}
}

func (m *TxStateMessage) DecodeMessage(message *sarama.ConsumerMessage) error {
	m.TxID = string(message.Key)
	if message.Value == nil {
		m.State = optional.Nil[TxState]()
		return nil
	}
	var state TxState
	err := json.Unmarshal(message.Value, &state)
	if err != nil {
		return err
	}
	m.State = optional.New(state)
	return nil
}
