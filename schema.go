package kaffe

import (
	"encoding/json"
	"time"

	"github.com/IBM/sarama"
	"github.com/leavez/go-optional"
)

type MessageEncoder interface {
	EncodeMessage() sarama.ProducerMessage
}

type MessageDecoder interface {
	DecodeMessage(message *sarama.ConsumerMessage) error
}

type BidEventMessage struct {
	txID  string
	event BidEvent
}

func (e BidEventMessage) EncodeMessage() sarama.ProducerMessage {
	b, _ := json.Marshal(e)
	return sarama.ProducerMessage{
		Key:   sarama.StringEncoder(e.txID),
		Value: sarama.ByteEncoder(b),
	}
}

func (e *BidEventMessage) DecodeMessage(message *sarama.ConsumerMessage) error {
	err := json.Unmarshal(message.Value, &e)
	if err != nil {
		return err
	}
	e.txID = string(message.Key)
	return nil
}

type DeliveryEventMessage struct {
	txID  string
	event DeliveryEvent
}

func (e DeliveryEventMessage) EncodeMessage() sarama.ProducerMessage {
	b, _ := json.Marshal(e)
	return sarama.ProducerMessage{
		Key:   sarama.StringEncoder(e.txID),
		Value: sarama.ByteEncoder(b),
	}
}

func (e DeliveryEventMessage) DecodeMessage(message *sarama.ConsumerMessage) error {
	err := json.Unmarshal(message.Value, &e)
	if err != nil {
		return err
	}
	e.txID = string(message.Key)
	return nil
}

type TxStateMessage struct {
	txID  string
	state optional.Type[TxState]
}

type TxState struct {
	CampaignID string    `json:"campaignID"`
	Quart      int8      `json:"quart"`
	BidTime    time.Time `json:"bidTime"`
}

func (m TxStateMessage) EncodeMessage() sarama.ProducerMessage {
	var v sarama.Encoder
	if m.state.IsNil() {
		v = nil
	} else {
		b, _ := json.Marshal(m.state.ForceValue())
		v = sarama.ByteEncoder(b)
	}
	return sarama.ProducerMessage{
		Key:   sarama.StringEncoder(m.txID),
		Value: v,
	}
}

func (m TxStateMessage) DecodeMessage(message *sarama.ConsumerMessage) error {
	m.txID = string(message.Key)
	if message.Value == nil {
		m.state = optional.Nil[TxState]()
		return nil
	}
	var state TxState
	err := json.Unmarshal(message.Value, &state)
	if err != nil {
		return err
	}
	m.state = optional.New(state)
	return nil
}
