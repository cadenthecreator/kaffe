package kaffe

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
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

func TestMissing(t *testing.T) {
	txStateLog := map[string]string{}
	event, stateUpdate := processBidEvent("TestId", BidEvent{
		quart:     4,
		cmpid:     "",
		eventType: PlaybackEvent,
	}, txStateLog)
	if !(event.eventType == None && stateUpdate.eventType == NoUpdate) {
		t.Errorf("Expected 0 and 0 but got %s and %s", strconv.Itoa(int(event.eventType)), strconv.Itoa(int(stateUpdate.eventType)))
	}
}

func TestIntegration(t *testing.T) {
	t.Log("testing queue")
	ctx := context.Background()
	kafkaContainer := spinUpKafkaContainer(ctx)
	// defer terminateKafkaContainer(kafkaContainer)

	// Connect producer and consumer group
	producer, consumerGroup := connectToKafkaContainer(ctx, kafkaContainer, "txStateLog-producer", "txStateLog-group")
	topic := "txStateLog"

	// Produce a test message
	txStateLog := map[string]string{}
	executeTxStateUpdate(TxStateUpdate{
		key:       "TestId",
		value:     "test",
		eventType: Update,
	}, txStateLog, producer, topic)

	// Use a WaitGroup to synchronize
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		loadFromTxStateLogWithGroup(consumerGroup, topic, func(key, value string) {
			if value == "" {
				delete(txStateLog, key)
			} else {
				txStateLog[key] = value
			}
		})
	}()

	// Wait for processing to complete
	wg.Wait()

	// Verify results
	if txStateLog["TestId"] != "test" {
		t.Errorf("Expected 'test' but got %s", txStateLog["TestId"])
	}
}

func TestKafkaQueue(t *testing.T) {
	t.Log("testing queue")
	ctx := context.Background()
	kafkaContainer := spinUpKafkaContainer(ctx)
	defer terminateKafkaContainer(kafkaContainer)

	// Connect producer and consumer group
	producer, consumerGroup := connectToKafkaContainer(ctx, kafkaContainer, "task-producer-id", "task-group")
	topic := "task-queue"

	// Ensure topic exists
	ensureTopicWithPartitions(kafkaContainer, ctx, topic, 3)

	// Produce tasks
	produceEvent(producer, topic, "task1-key", "Process task 1")
	produceEvent(producer, topic, "task2-key", "Process task 2")
	produceEvent(producer, topic, "task3-key", "Process task 3")

	// Consume tasks and log output
	go consumeEvents(consumerGroup, topic, func(key, value string) {
		t.Logf("Task processed: key=%s, value=%s", key, value)
	})

	// Allow time for processing
	time.Sleep(5 * time.Second)
}
