package kaffe

import (
	"context"
	"log"

	"strings"
	"testing"
	"time"

	model "kaffe/test_models"

	"github.com/IBM/sarama"
	"github.com/leavez/go-optional"
	"github.com/stretchr/testify/assert"
)

type DeliveryAggregation struct {
	state       model.StateMachine
	stateTopic  string
	outputTopic string
}

func NewDeliveryAggregation(stateTopic string, outputTopic string) StateTransducer {
	return &DeliveryAggregation{
		stateTopic:  stateTopic,
		outputTopic: outputTopic,
	}
}

func (d *DeliveryAggregation) LoadFrom(consumer StateConsumer, partition int32) error {
	stateCount := 0
	d.state = model.NewStateMachine()
	for {
		stateCount++
		m, err := consumer.NextMessage()
		if err != nil {
			log.Fatalf("DeliveryAggregation ConsumeClaim consumeDeliveryState offset: %d, error: %s", m.Offset, err)
		}
		if m == nil {
			break
		}
		// log.Printf("LoadState count: %d last offset: %d", stateCount, m.Offset)
		var message model.TxStateMessage
		err = message.DecodeMessage(m)
		if err != nil {
			log.Printf("ConsumeClaim DecodeMessage offset: %d, error: %s", m.Offset, err)
		}
		d.state.LoadState(message)
	}
	log.Printf("LoadState completed")
	return nil
}

func (d *DeliveryAggregation) ProcessMessage(input *sarama.ConsumerMessage) (messages []*sarama.ProducerMessage, err error) {
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

		initialState := []model.TxStateMessage{
			{TxID: "tx1", State: optional.New(model.TxState{CampaignID: "test-campaign-id", Quart: -1})},
			{TxID: "tx1", State: optional.Nil[model.TxState]()},
			{TxID: "tx2", State: optional.New(model.TxState{CampaignID: "test-campaign-id", Quart: -1})},
		}
		for _, message := range initialState {
			if _, _, err = producer.SendMessage(message.EncodeMessage(configuration.StateTopic)); err != nil {
				log.Fatalf("failed to SendMessage: %s", err)
			}
		}

		bidEvents := []model.BidEventMessage{
			{TxID: "tx1", Event: model.BidEvent{EventType: model.PlaybackEvent, Quart: 0}},
			{TxID: "tx2", Event: model.BidEvent{EventType: model.PlaybackEvent, Quart: 0}},
			{TxID: "tx2", Event: model.BidEvent{EventType: model.PlaybackEvent, Quart: 1}},
			{TxID: "tx2", Event: model.BidEvent{EventType: model.PlaybackEvent, Quart: 4}},
		}
		for _, message := range bidEvents {
			if _, _, err = producer.SendMessage(message.EncodeMessage(configuration.InputTopic)); err != nil {
				log.Fatalf("failed to SendMessage: %s", err)
			}
		}
		t.Log("Completed")
	}()

	aggregation := NewDeliveryAggregation(configuration.StateTopic, configuration.OutputTopic)
	transducer := NewSaramaTransducer(configuration, aggregation)
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
				var message model.DeliveryEventMessage
				message.DecodeMessage(m)
				log.Printf("Received event %d key: %s, message: %v", eventCount, message.TxID, message.Event)
				eventCount++
			case _ = <-events.Errors():
				return
			case m := <-state.Messages():
				var message model.TxStateMessage
				message.DecodeMessage(m)
				log.Printf("Received state %d key: %s, message: %v", stateCount, message.TxID, message.State)
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

	transducer.Run(ctx)

	assert.Equal(t, 2, eventCount)
	assert.Equal(t, 2, stateCount)
}

// sigusr1     chan os.Signal
// sigterm     chan os.Signal
//
// sigusr1 := make(chan os.Signal, 1)
// signal.Notify(sigusr1, syscall.SIGUSR1)
// sigterm := make(chan os.Signal, 1)
// signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
//
// keepRunning := true
// isPaused := false
// for keepRunning {
// 	select {
// 	case <- d.ctx.Done():
// 		log.Println("terminating: context cancelled")
// 		keepRunning = false
// 	case <-d.sigterm:
// 		log.Println("terminating: via signal")
// 		keepRunning = false
// 	case <-d.sigusr1:
// 		if isPaused {
// 			d.client.ResumeAll()
// 			log.Println("Resuming consumption")
// 		} else {
// 			d.client.PauseAll()
// 			log.Println("Pausing consumption")
// 		}
// 		isPaused = !isPaused
// 	}
// }
// d.cancel()
