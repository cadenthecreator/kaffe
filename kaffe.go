package kaffe

import (
	"context"
	"log"

	kafgo "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
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

	//TxStateUpdate types
	NoUpdate  TxStateUpdateType = 0
	Update    TxStateUpdateType = 1
	Tombstone TxStateUpdateType = 2
)

type BidEvent struct {
	cmpid     string
	quart     int8
	eventType BidEventType
}

type DeliveryEvent struct {
	cmpid     string
	eventType DeliveryEventType
}

type TxStateUpdate struct {
	key       string
	value     string
	eventType TxStateUpdateType
}

func spinUpKafkaContainer(ctx context.Context) *kafka.KafkaContainer {
	kafkaContainer, err := kafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		kafka.WithClusterID("test-cluster"),
	)
	if err != nil {
		log.Fatalf("failed to start container: %s", err)
		return nil
	} else {
		log.Println("Started container")
	}
	return kafkaContainer
}

func terminateKafkaContainer(container *kafka.KafkaContainer) {
	if err := testcontainers.TerminateContainer(container); err != nil {
		log.Fatalf("failed to terminate container: %s", err)
	} else {
		log.Println("Stopped container")
	}
}

func connectToKafkaContainer(ctx context.Context, container *kafka.KafkaContainer, topic string, partition int) *kafgo.Conn {
	brokers, err := container.Brokers(ctx)
	if err != nil {
		log.Fatalf("failed to get brokers: %s", err)
	} else {
		log.Println("Got kafka cluster brokers")
	}
	broker := brokers[0]

	conn, err := kafgo.DialLeader(ctx, "tcp", broker, topic, partition)
	if err != nil {
		log.Fatalf("failed to dial leader: %s", err)
	} else {
		log.Println("Dialed Leader")
	}
	return conn
}

func loadFromTxStateLog(conn *kafgo.Conn, partition int, topic string) map[string]string {
	r := kafgo.NewReader(kafgo.ReaderConfig{
		Brokers:   []string{conn.Broker().Host},
		Topic:     topic,
		Partition: partition,
	})
	txStateLog := map[string]string{}
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		if m.Value == nil {
			delete(txStateLog, string(m.Key))
		} else {
			txStateLog[string(m.Key)] = string(m.Value)
		}
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
	return txStateLog
}

func processBidEvent(TxID string, event BidEvent, txStateLog map[string]string) (DeliveryEvent, TxStateUpdate) {
	_, hastx := txStateLog[TxID]
	if event.eventType == BidResultEvent {
		return DeliveryEvent{
				cmpid:     event.cmpid,
				eventType: None,
			}, TxStateUpdate{
				key:       TxID,
				value:     event.cmpid,
				eventType: Update,
			}

	} else if event.quart == 0 && hastx {
		return DeliveryEvent{
				cmpid:     txStateLog[TxID],
				eventType: ImpressionEvent,
			}, TxStateUpdate{
				key:       "",
				value:     "",
				eventType: NoUpdate,
			}
	} else if event.quart == 4 && hastx {
		return DeliveryEvent{
				cmpid:     txStateLog[TxID],
				eventType: CompletionEvent,
			}, TxStateUpdate{
				key:       TxID,
				value:     "",
				eventType: Tombstone,
			}
	}
	return DeliveryEvent{
			cmpid:     event.cmpid,
			eventType: None,
		}, TxStateUpdate{
			key:       "",
			value:     "",
			eventType: NoUpdate,
		}
}

func executeTxStateUpdate(txStateUpdate TxStateUpdate, txStateLog map[string]string, conn *kafgo.Conn, topic string, patrition int) {
	if txStateUpdate.eventType == Update {
		txStateLog[txStateUpdate.key] = txStateUpdate.value
		msg := kafgo.Message{
			Key:       []byte(txStateUpdate.key),
			Value:     []byte(txStateUpdate.value),
			Topic:     topic,
			Partition: patrition,
		}
		conn.WriteMessages(msg)
	} else if txStateUpdate.eventType == Tombstone {
		delete(txStateLog, txStateUpdate.key)
		msg := kafgo.Message{
			Key:       []byte(txStateUpdate.key),
			Topic:     topic,
			Partition: patrition,
		}
		conn.WriteMessages(msg)
	}
}
