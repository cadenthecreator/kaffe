package kaffe

import (
	"strconv"
	"testing"
)

func TestBid(t *testing.T) {
	txStateLog := map[string]string{}
	event, stateUpdate := processBidEvent("TestId", BidEvent{
		quart:     0,
		cmpid:     "test",
		eventType: BidResultEvent,
	}, txStateLog)
	if !(event.eventType == None && stateUpdate.eventType == Update) {
		t.Errorf("Expected 0 and 1 but got %s and %s", strconv.Itoa(int(event.eventType)), strconv.Itoa(int(stateUpdate.eventType)))
	}
}

func TestImpression(t *testing.T) {
	txStateLog := map[string]string{"TestId": "test"}
	event, stateUpdate := processBidEvent("TestId", BidEvent{
		quart:     0,
		cmpid:     "",
		eventType: PlaybackEvent,
	}, txStateLog)
	if !(event.eventType == ImpressionEvent && stateUpdate.eventType == NoUpdate) {
		t.Errorf("Expected 1 and 0 but got %s and %s", strconv.Itoa(int(event.eventType)), strconv.Itoa(int(stateUpdate.eventType)))
	}
}

func TestComplete(t *testing.T) {
	txStateLog := map[string]string{"TestId": "test"}
	event, stateUpdate := processBidEvent("TestId", BidEvent{
		quart:     4,
		cmpid:     "",
		eventType: PlaybackEvent,
	}, txStateLog)
	if !(event.eventType == CompletionEvent && stateUpdate.eventType == Tombstone) {
		t.Errorf("Expected 2 and 2 but got %s and %s", strconv.Itoa(int(event.eventType)), strconv.Itoa(int(stateUpdate.eventType)))
	}
}
