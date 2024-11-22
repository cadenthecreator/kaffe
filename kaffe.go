package kaffe

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"log"
	"strings"
	"time"
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

	// TxStateUpdate types
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

func produceEvent(producer sarama.SyncProducer, topic, key, value string) {
	// Begin transaction
	if err := producer.BeginTxn(); err != nil {
		log.Fatalf("Failed to begin transaction: %s", err)
	}
	log.Println("Transaction started for producing event")

	// Prepare the message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}

	// Send the message
	_, _, err := producer.SendMessage(msg)
	if err != nil {
		// Abort the transaction if sending fails
		if abortErr := producer.AbortTxn(); abortErr != nil {
			log.Fatalf("Failed to abort transaction: %s", abortErr)
		}
		log.Fatalf("Failed to send event: %s", err)
	}
	log.Printf("Event sent to topic %s with key=%s and value=%s", topic, key, value)

	// Commit the transaction
	if err := producer.CommitTxn(); err != nil {
		log.Fatalf("Failed to commit transaction: %s", err)
	}
	log.Println("Transaction committed for event")
}

type EventConsumer struct {
	ProcessEvent func(key, value string)
}

func (c *EventConsumer) Setup(_ sarama.ConsumerGroupSession) error {
	log.Println("Consumer group session started")
	return nil
}

func (c *EventConsumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	log.Println("Consumer group session ended")
	return nil
}

func (c *EventConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("Event received: key=%s, value=%s", string(msg.Key), string(msg.Value))
		c.ProcessEvent(string(msg.Key), string(msg.Value))
		sess.MarkMessage(msg, "") // Commit offset after processing
	}
	return nil
}

func consumeEvents(consumerGroup sarama.ConsumerGroup, topic string, processEvent func(key, value string)) {
	handler := &EventConsumer{ProcessEvent: processEvent}

	for {
		// Start consuming from the topic
		if err := consumerGroup.Consume(context.Background(), []string{topic}, handler); err != nil {
			log.Fatalf("Error consuming events: %s", err)
		}
	}
}

// Spin up a Kafka container
func spinUpKafkaContainer(ctx context.Context) *kafka.KafkaContainer {
	kafkaContainer, err := kafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		kafka.WithClusterID("test-cluster"),
	)
	if err != nil {
		log.Fatalf("failed to start container: %s", err)
	}
	log.Println("Started Kafka container")
	return kafkaContainer
}

// Terminate the Kafka container
func terminateKafkaContainer(container *kafka.KafkaContainer) {
	if err := testcontainers.TerminateContainer(container); err != nil {
		log.Fatalf("failed to terminate container: %s", err)
	} else {
		log.Println("Stopped Kafka container")
	}
}

// Connect to Kafka container and return both producer and consumer

func waitForKafkaReadiness(brokers []string) error {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return err
	}
	defer client.Close()
	log.Println("Kafka is ready")
	return nil
}

func connectToKafkaContainer(ctx context.Context, container *kafka.KafkaContainer, transactionalID, groupID string) (sarama.SyncProducer, sarama.ConsumerGroup) {
	brokers, err := container.Brokers(ctx)
	if err != nil {
		log.Fatalf("Failed to get brokers: %s", err)
	}
	log.Printf("Kafka brokers: %v", brokers)

	// Create transactional producer
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.Idempotent = true
	producerConfig.Producer.Transaction.ID = transactionalID
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producerConfig.Net.MaxOpenRequests = 1

	producer, err := sarama.NewSyncProducer(brokers, producerConfig)
	if err != nil {
		log.Fatalf("Failed to create transactional producer: %s", err)
	}
	log.Println("Transactional Kafka producer connected")

	// Create consumer group
	consumerConfig := sarama.NewConfig()
	consumerConfig.Version = sarama.V2_8_0_0
	consumerConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerConfig.Consumer.IsolationLevel = sarama.ReadCommitted // Read committed messages only

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, consumerConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %s", err)
	}
	log.Println("Kafka consumer group connected")

	return producer, consumerGroup
}

// Ensure the topic is created with the given number of partitions
func ensureTopicWithPartitions(container *kafka.KafkaContainer, ctx context.Context, topic string, numPartitions int32) {
	brokers, err := container.Brokers(ctx)
	if err != nil {
		log.Fatalf("failed to get brokers: %s", err)
	}

	admin, err := sarama.NewClusterAdmin(brokers, sarama.NewConfig())
	if err != nil {
		log.Fatalf("failed to create cluster admin: %s", err)
	}
	defer admin.Close()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     numPartitions,
		ReplicationFactor: 1,
	}, false)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		log.Fatalf("failed to create topic: %s", err)
	}
	log.Printf("Topic %s is ready with %d partitions", topic, numPartitions)
}

// Execute a transaction state update with transactions
func executeTxStateUpdate(txStateUpdate TxStateUpdate, txStateLog map[string]string, producer sarama.SyncProducer, topic string) {
	// Begin transaction
	err := producer.BeginTxn()
	if err != nil {
		log.Fatalf("failed to begin transaction: %s", err)
	}

	partitionKey := txStateUpdate.key // Use the key to determine partition
	var msg *sarama.ProducerMessage

	// Handle tombstone messages explicitly
	if txStateUpdate.eventType == Tombstone {
		msg = &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(partitionKey),
			Value: nil, // Null value indicates a tombstone message
		}
		delete(txStateLog, txStateUpdate.key) // Remove from local state log
		log.Printf("Tombstone message prepared for key: %s", partitionKey)
	} else {
		// Handle regular updates
		msg = &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(partitionKey),
			Value: sarama.StringEncoder(txStateUpdate.value),
		}
		txStateLog[txStateUpdate.key] = txStateUpdate.value // Update local state log
		log.Printf("Update message prepared for key: %s, value: %s", partitionKey, txStateUpdate.value)
	}

	// Send the message
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("failed to send message: %s", err)
	}

	// Commit transaction
	err = producer.CommitTxn()
	if err != nil {
		log.Fatalf("failed to commit transaction: %s", err)
	}
	log.Printf("Transaction committed for topic %s with key %s", topic, partitionKey)
}

func loadFromTxStateLog(consumer sarama.Consumer, topic string, partition int32) map[string]string {
	log.Println("Starting loadFromTxStateLog...")

	// Consume messages from the specified partition
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to consume partition: %s", err)
	}
	defer partitionConsumer.Close()

	txStateLog := make(map[string]string)
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Println("Reading messages from partition...")
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			if msg.Value == nil {
				delete(txStateLog, string(msg.Key))
			} else {
				txStateLog[string(msg.Key)] = string(msg.Value)
			}
		case <-timeoutCtx.Done():
			log.Println("Timeout reached while reading messages")
			return txStateLog
		}
	}
}

func loadFromTxStateLogWithGroup(consumerGroup sarama.ConsumerGroup, topic string, processMessage func(key, value string)) map[string]string {
	log.Println("Starting loadFromTxStateLogWithGroup...")

	stateLog := make(map[string]string)
	handler := &EventConsumer{
		ProcessEvent: func(key, value string) {
			if value == "" {
				delete(stateLog, key)
			} else {
				stateLog[key] = value
			}
		},
	}

	// Start consuming messages
	go func() {
		for {
			if err := consumerGroup.Consume(context.Background(), []string{topic}, handler); err != nil {
				log.Fatalf("Error consuming messages: %s", err)
			}
		}
	}()

	// Allow some time for processing
	time.Sleep(5 * time.Second)
	return stateLog
}

// Process a bid event and update transaction state
func processBidEvent(TxID string, event BidEvent, txStateLog map[string]string) (DeliveryEvent, TxStateUpdate) {
	_, hasTx := txStateLog[TxID]
	if event.eventType == BidResultEvent {
		return DeliveryEvent{
				cmpid:     event.cmpid,
				eventType: None,
			}, TxStateUpdate{
				key:       TxID,
				value:     event.cmpid,
				eventType: Update,
			}
	} else if event.quart == 0 && hasTx {
		return DeliveryEvent{
				cmpid:     txStateLog[TxID],
				eventType: ImpressionEvent,
			}, TxStateUpdate{
				key:       "",
				value:     "",
				eventType: NoUpdate,
			}
	} else if event.quart == 4 && hasTx {
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
