package kaffe

import (
	"context"
	"log"

	"strings"
	"testing"
	"time"

	container "kaffe/test_container"
	model "kaffe/test_models"

	"github.com/IBM/sarama"
	"github.com/leavez/go-optional"
	"github.com/stretchr/testify/assert"
)

func TestSaramaTransducer(t *testing.T) {
	ctx := context.Background()

	t.Log("Startting kafka...")
	kafkaContainer := container.SpinUpKafkaContainer(ctx)
	defer func() {
		t.Log("Stopping kafka...")
		container.TerminateKafkaContainer(kafkaContainer)
	}()

	brokers, _ := kafkaContainer.Brokers(ctx)
	transactionalId := "test-transactional-id." + time.Now().String()
	configuration := NewConfiguartion(brokers, transactionalId, "test-namespace")

	t.Log("Creating topics...")
	setupTestTopics(configuration)

	t.Log("Initializing test state...")
	setupTestData(configuration,
		[]model.TxStateMessage{
			{TxID: "tx1", State: optional.New(model.TxState{CampaignID: "test-campaign-id", Quart: -1})},
			{TxID: "tx1", State: optional.Nil[model.TxState]()},
			{TxID: "tx2", State: optional.New(model.TxState{CampaignID: "test-campaign-id", Quart: -1})},
		},
		[]model.BidEventMessage{
			{TxID: "tx1", Event: model.BidEvent{EventType: model.PlaybackEvent, Quart: 0}},
			{TxID: "tx2", Event: model.BidEvent{EventType: model.PlaybackEvent, Quart: 0}},
			{TxID: "tx2", Event: model.BidEvent{EventType: model.PlaybackEvent, Quart: 1}},
			{TxID: "tx2", Event: model.BidEvent{EventType: model.PlaybackEvent, Quart: 4}},
		},
	)

	ctx, cancel := context.WithCancel(ctx)

	t.Log("Start consuming output...")
	eventCount := 0
	stateCount := 0
	startConsumingOutput(configuration, func(state sarama.PartitionConsumer, events sarama.PartitionConsumer) {
		for {
			select {
			case m := <-events.Messages():
				var message model.DeliveryEventMessage
				message.DecodeMessage(m)
				t.Logf("Received event %d key: %s, message: %v", eventCount, message.TxID, message.Event)
				eventCount++
			case _ = <-events.Errors():
				return
			case m := <-state.Messages():
				var message model.TxStateMessage
				message.DecodeMessage(m)
				t.Logf("Received state %d key: %s, message: %v", stateCount, message.TxID, message.State)
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
	})

	t.Log("Running test...")
	stateTransducer := newTestStateTransducer(configuration.StateTopic, configuration.OutputTopic)
	transducer := NewSaramaTransducer(configuration, stateTransducer)
	transducer.Run(ctx)

	assert.Equal(t, 2, eventCount)
	assert.Equal(t, 2, stateCount)
	t.Log("Completed")
}

type testStateTransducer struct {
	state       model.StateMachine
	stateTopic  string
	outputTopic string
}

func newTestStateTransducer(stateTopic string, outputTopic string) StateTransducer {
	return &testStateTransducer{
		stateTopic:  stateTopic,
		outputTopic: outputTopic,
	}
}

func (d *testStateTransducer) LoadFrom(consumer StateConsumer, partition int32) error {
	stateCount := 0
	d.state = model.NewStateMachine()
	for {
		stateCount++
		m, err := consumer.NextMessage()
		if err != nil {
			log.Fatalf("LoadFrom StateConsumer partition %d offset: %d, error: %s", partition, m.Offset, err)
		}
		if m == nil {
			break
		}
		// log.Printf("LoadState count: %d last offset: %d", stateCount, m.Offset)
		var message model.TxStateMessage
		err = message.DecodeMessage(m)
		if err != nil {
			log.Printf("LoadFrom StateConsumer DecodeMessage partition %d offset: %d, error: %s", partition, m.Offset, err)
		}
		d.state.LoadState(message)
	}
	// log.Printf("LoadState completed")
	return nil
}

func (d *testStateTransducer) ProcessMessage(input *sarama.ConsumerMessage) (messages []*sarama.ProducerMessage, err error) {
	var message model.BidEventMessage
	err = message.DecodeMessage(input)
	if err != nil {
		log.Printf("DecodeMessage error: %s", err)
		return
	}
	state, output, err := d.state.HandleBidEvent(message)
	if err != nil {
		log.Printf("HandleBidEvent error: %s", err)
		return
	}
	if state != nil {
		messages = append(messages, state.EncodeMessage(d.stateTopic))
	}
	if output != nil {
		messages = append(messages, output.EncodeMessage(d.outputTopic))
	}
	return
}

func setupTestTopics(configuration Configuration) {
	admin, err := sarama.NewClusterAdmin(configuration.Brokers, configuration.State)
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
}

func setupTestData(configuration Configuration, state []model.TxStateMessage, input []model.BidEventMessage) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	producer, err := sarama.NewSyncProducer(configuration.Brokers, config)
	if err != nil {
		log.Fatalf("failed to create producer: %s", err)
	}
	defer producer.Close()

	for _, message := range state {
		if _, _, err = producer.SendMessage(message.EncodeMessage(configuration.StateTopic)); err != nil {
			log.Fatalf("failed to SendMessage: %s", err)
		}
	}

	for _, message := range input {
		if _, _, err = producer.SendMessage(message.EncodeMessage(configuration.InputTopic)); err != nil {
			log.Fatalf("failed to SendMessage: %s", err)
		}
	}
}

func startConsumingOutput(configuration Configuration, do func(state sarama.PartitionConsumer, events sarama.PartitionConsumer)) {
	consumer, err := sarama.NewConsumer(configuration.Brokers, configuration.State)
	if err != nil {
		log.Fatalf("failed to create consumer: %s", err)
	}

	events, err := consumer.ConsumePartition(configuration.OutputTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("failed to create consumer: %s", err)
	}

	state, err := consumer.ConsumePartition(configuration.StateTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("failed to create consumer: %s", err)
	}

	go func() {
		defer consumer.Close()
		defer events.Close()
		defer state.Close()
		do(state, events)
	}()
}
