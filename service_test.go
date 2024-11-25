package kaffe

import (
	"context"
	"log"

	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/leavez/go-optional"
	"github.com/stretchr/testify/assert"
)

func TestDeliveryAggregation(t *testing.T) {
	t.Log("testing DeliveryAggregation")
	ctx := context.Background()
	kafkaContainer := spinUpKafkaContainer(ctx)
	defer terminateKafkaContainer(kafkaContainer)

	brokers, _ := kafkaContainer.Brokers(ctx)
	transactionalId := "test-transactional-id." + time.Now().String()
	configuration := NewConfiguartion(brokers, transactionalId, "test-namespace")

	client, err := sarama.NewClient(brokers, configuration.State)
	if err != nil {
		log.Fatalf("failed to create NewClient admin: %s", err)
	}

	func() {
		admin, err := sarama.NewClusterAdminFromClient(client)
		if err != nil {
			log.Fatalf("failed to create cluster admin: %s", err)
		}
		defer admin.Close()

		err = admin.CreateTopic(configuration.InputTopic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			log.Fatalf("failed to create topic: %s", err)
		}

		err = admin.CreateTopic(configuration.OutputTopic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			log.Fatalf("failed to create topic: %s", err)
		}

		err = admin.CreateTopic(configuration.StateTopic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			log.Fatalf("failed to create topic: %s", err)
		}
	}()

	func() {
		t.Log("Setingup topics...")

		config := sarama.NewConfig()
		config.Version = sarama.V2_8_0_0
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		producer, err := sarama.NewSyncProducer(configuration.Brokers, config)
		if err != nil {
			log.Fatalf("failed to create producer: %s", err)
		}
		defer producer.Close()

		initialState := []TxStateMessage{
			{txID: "tx1", state: optional.New(TxState{CampaignID: "test-campaign-id", Quart: -1})},
			{txID: "tx1", state: optional.Nil[TxState]()},
			{txID: "tx2", state: optional.New(TxState{CampaignID: "test-campaign-id", Quart: -1})},
		}
		for _, message := range initialState {
			if _, _, err = producer.SendMessage(message.EncodeMessage(configuration.StateTopic)); err != nil {
				log.Fatalf("failed to SendMessage: %s", err)
			}
		}

		bidEvents := []BidEventMessage{
			{txID: "tx1", event: BidEvent{EventType: PlaybackEvent, Quart: 0}},
			{txID: "tx2", event: BidEvent{EventType: PlaybackEvent, Quart: 0}},
			{txID: "tx2", event: BidEvent{EventType: PlaybackEvent, Quart: 1}},
			{txID: "tx2", event: BidEvent{EventType: PlaybackEvent, Quart: 4}},
		}
		for _, message := range bidEvents {
			if _, _, err = producer.SendMessage(message.EncodeMessage(configuration.InputTopic)); err != nil {
				log.Fatalf("failed to SendMessage: %s", err)
			}
		}
		t.Log("Completed")
	}()

	service := NewDeliveryAggregation(configuration)
	ctx, cancel := context.WithCancel(ctx)

	consumer, err := sarama.NewConsumer(configuration.Brokers, configuration.State)
	if err != nil {
		log.Fatalf("failed to create consumer: %s", err)
	}
	defer consumer.Close()

	events, err := consumer.ConsumePartition(configuration.OutputTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("failed to create consumer: %s", err)
	}
	defer events.Close()

	state, err := consumer.ConsumePartition(configuration.StateTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("failed to create consumer: %s", err)
	}
	defer state.Close()

	eventCount := 0
	stateCount := 0
	go func() {
		for {
			select {
			case m := <-events.Messages():
				var message DeliveryEventMessage
				message.DecodeMessage(m)
				log.Printf("Received event %d key: %s, message: %v", eventCount, message.txID, message.event)
				eventCount++
			case _ = <-events.Errors():
				return
			case m := <-state.Messages():
				var message TxStateMessage
				message.DecodeMessage(m)
				log.Printf("Received state %d key: %s, message: %v", stateCount, message.txID, message.state)
				stateCount++
			case _ = <-state.Errors():
				return
			case <-ctx.Done():
				return
			}
			if eventCount == 2 && stateCount == 2 {
				cancel()
			}
		}
	}()

	service.Run(ctx)

	assert.Equal(t, 2, eventCount)
	assert.Equal(t, 2, stateCount)
}
