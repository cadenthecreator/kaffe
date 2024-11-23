package kaffe

import (
	"context"
	"errors"
	"log"

	"github.com/IBM/sarama"
)

type DeliveryAggregation struct {
	client        sarama.Client
	configuration Configuartion
}

type Configuartion struct {
	Brokers             []string
	Sarama              *sarama.Config
	bidEventsTopic      string
	deliveryEventsTopic string
	deliveryStateTopic  string
}

func NewConfiguartion(brokers []string, transactionalID string, namespace string) Configuartion {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.IsolationLevel = sarama.ReadCommitted

	config.Producer.Return.Successes = true
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = transactionalID
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Net.MaxOpenRequests = 1

	return Configuartion{
		Brokers:             brokers,
		Sarama:              config,
		bidEventsTopic:      namespace + "." + "bid-events",
		deliveryEventsTopic: namespace + "." + "delivery-events",
		deliveryStateTopic:  namespace + "." + "delivery-state",
	}
}

func New(config Configuartion) DeliveryAggregation {

	client, err := sarama.NewClient(config.Brokers, config.Sarama)
	if err != nil {
		log.Fatalf("DeliveryAggregation NewClient %s", err)
	}

	return DeliveryAggregation{
		client: client,
	}
}

func (d *DeliveryAggregation) Run(ctx context.Context) {
	group, err := sarama.NewConsumerGroupFromClient(DeliveryAggregationConsumerGroup, d.client)
	if err != nil {
		log.Fatalf("Error NewConsumerGroupFromClient: %v", err)
	}

	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := group.Consume(ctx, []string{d.configuration.bidEventsTopic}, d); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}
			log.Panicf("Error from consumer: %v", err)
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			return
		}
	}
}

const DeliveryAggregationConsumerGroup = "deliveryAggregation"

func (d *DeliveryAggregation) Setup(_ sarama.ConsumerGroupSession) error {
	log.Println("DeliveryAggregation ConsumerGroupSession Setup")
	return nil
}

func (d *DeliveryAggregation) Cleanup(_ sarama.ConsumerGroupSession) error {
	log.Println("DeliveryAggregation ConsumerGroupSession Cleanup")
	return nil
}

func (d *DeliveryAggregation) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (err error) {
	log.Println("DeliveryAggregation ConsumerGroupSession ConsumeClaim")

	partition := claim.Partition()
	state := StateMachine{}
	consumer, err := sarama.NewConsumerFromClient(d.client)
	if err != nil {
		log.Fatalf("DeliveryAggregation NewConsumerFromClient %s", err)
	}
	defer consumer.Close()

	stateConsumer, err := consumer.ConsumePartition(d.configuration.deliveryStateTopic, partition, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("DeliveryAggregation NewConsumerFromClient %s", err)
	}
	defer stateConsumer.Close()

	for m := range stateConsumer.Messages() {
		var message TxStateMessage
		err := message.DecodeMessage(m)
		if err != nil {
			log.Printf("DeliveryAggregation DecodeFrom %s", err)
		} else {
			state.LoadState(message)
		}
	}

	producer, err := sarama.NewSyncProducerFromClient(d.client)
	if err != nil {
		log.Fatalf("DeliveryAggregation NewSyncProducerFromClient %s", err)
	}
	defer producer.Close()

	for {
		select {
		case m, ok := <-claim.Messages():
			if !ok {
				break
			}
			var message BidEventMessage
			err := message.DecodeMessage(m)
			if err != nil {
				log.Printf("DeliveryAggregation DecodeFrom %s", err)
			} else {
				event, state, err := state.HandleBidEvent(message)
				if err != nil {
					log.Printf("DeliveryAggregation HandleBidEvent %s", err)
				} else if event.IsNil() && state.IsNil() {
					log.Printf("DeliveryAggregation delivery.IsNil() && state.IsNil()")
					continue
				} else {
					// Begin transaction
					if err := producer.BeginTxn(); err != nil {
						log.Fatalf("Failed to begin transaction: %s", err)
					}

					if !event.IsNil() {
						m := event.ForceValue().EncodeMessage()
						m.Topic = d.configuration.deliveryEventsTopic
						_, _, err := producer.SendMessage(&m)
						if err != nil {
							producer.AbortTxn()
							log.Fatalf("Failed to SendMessage for delivery: %s", err)
						}

					}
					if !state.IsNil() {
						m := state.ForceValue().EncodeMessage()
						m.Topic = d.configuration.deliveryStateTopic
						_, _, err := producer.SendMessage(&m)
						if err != nil {
							producer.AbortTxn()
							log.Fatalf("Failed to SendMessage for delivery: %s", err)
						}
					}

					err = producer.AddMessageToTxn(m, DeliveryAggregationConsumerGroup, nil)

					if err != nil {
						producer.AbortTxn()
						log.Fatalf("Failed to AddMessageToTxn: %s", err)
					}

					// Commit the transaction
					if err := producer.CommitTxn(); err != nil {
						log.Fatalf("Failed to commit transaction: %s", err)
					}
				}
			}
		case <-session.Context().Done():
			break
		}
	}
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
