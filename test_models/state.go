package test_models

import (
	"github.com/leavez/go-optional"
)

func NewStateMachine() StateMachine {
	return StateMachine{
		transactions: make(map[string]TxState),
	}
}

type StateMachine struct {
	transactions map[string]TxState
}

func (s *StateMachine) LoadState(message TxStateMessage) {
	if message.State.IsNil() {
		delete(s.transactions, message.TxID)
	} else if message.TxID != "" {
		s.transactions[message.TxID] = message.State.ForceValue()
	}
}

func (s *StateMachine) HandleBidEvent(message BidEventMessage) (state *TxStateMessage, output *DeliveryEventMessage, err error) {
	txID := message.TxID
	event := message.Event
	txState, hasTx := s.transactions[txID]
	if hasTx && event.EventType == BidResultEvent {
		err = nil // TODO: return proper error
	} else if !hasTx && event.EventType != BidResultEvent {
		err = nil // TODO: return proper error
	} else if event.EventType == BidResultEvent {
		txState.CampaignID = event.CampaignID
		txState.Quart = -1
		state = s.updateTx(txID, txState)
	} else if event.EventType == PlaybackEvent {
		if txState.Quart < 0 && event.Quart == 0 {
			txState.Quart = 0
			state = s.updateTx(txID, txState)
			output = s.impressionEvent(txID, txState)
		} else if event.Quart == 4 {
			output = s.completionEvent(txID, txState)
			state = s.tombstoneTx(txID)
		}
	}
	return
}

func (s *StateMachine) impressionEvent(txID string, txState TxState) *DeliveryEventMessage {
	return &DeliveryEventMessage{
		TxID:  txID,
		Event: DeliveryEvent{CampaignID: txState.CampaignID, EventType: ImpressionEvent},
	}
}

func (s *StateMachine) completionEvent(txID string, txState TxState) *DeliveryEventMessage {
	return &DeliveryEventMessage{
		TxID:  txID,
		Event: DeliveryEvent{CampaignID: txState.CampaignID, EventType: CompletionEvent},
	}
}

func (s *StateMachine) tombstoneTx(txID string) *TxStateMessage {
	delete(s.transactions, txID)
	return &TxStateMessage{
		TxID:  txID,
		State: optional.Nil[TxState](),
	}
}

func (s *StateMachine) updateTx(txID string, state TxState) *TxStateMessage {
	s.transactions[txID] = state
	return &TxStateMessage{
		TxID:  txID,
		State: optional.New(state),
	}
}
