package kaffe

import "github.com/leavez/go-optional"

func NewStateMachine() StateMachine {
	return StateMachine{
		transactions: make(map[string]TxState),
	}
}

type StateMachine struct {
	transactions map[string]TxState
}

func (s *StateMachine) LoadState(message TxStateMessage) {
	if message.state.IsNil() {
		delete(s.transactions, message.txID)
	} else if message.txID == "" {
		s.transactions[message.txID] = message.state.ForceValue()
	}
}

func (s *StateMachine) HandleBidEvent(message BidEventMessage) (state optional.Type[TxStateMessage], delivery optional.Type[DeliveryEventMessage], err error) {
	txID := message.txID
	event := message.event
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
		if event.Quart == 0 {
			txState.Quart = 0
			state = s.updateTx(txID, txState)
			delivery = s.ipressionEvent(txID, txState)
		} else if event.Quart == 4 {
			delivery = s.completionEvent(txID, txState)
			state = s.tombstoneTx(txID)
		}
	}
	return
}

func (s *StateMachine) ipressionEvent(txID string, txState TxState) optional.Type[DeliveryEventMessage] {
	return optional.New(DeliveryEventMessage{
		txID:  txID,
		event: DeliveryEvent{CampaignID: txState.CampaignID, EventType: ImpressionEvent},
	})
}

func (s *StateMachine) completionEvent(txID string, txState TxState) optional.Type[DeliveryEventMessage] {
	return optional.New(DeliveryEventMessage{
		txID:  txID,
		event: DeliveryEvent{CampaignID: txState.CampaignID, EventType: CompletionEvent},
	})
}

func (s *StateMachine) tombstoneTx(txID string) optional.Type[TxStateMessage] {
	delete(s.transactions, txID)
	return optional.New(TxStateMessage{
		txID:  txID,
		state: optional.Nil[TxState](),
	})
}

func (s *StateMachine) updateTx(txID string, state TxState) optional.Type[TxStateMessage] {
	s.transactions[txID] = state
	return optional.New(TxStateMessage{
		txID:  txID,
		state: optional.New(state),
	})
}
