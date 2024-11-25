package kaffe

import (
	"context"
	"errors"
	"log"

	"github.com/IBM/sarama"
)

type DeliveryAggregation struct {
	configuration Configuration
	group         sarama.ConsumerGroup
}

type Configuration struct {
	Brokers     []string
	State       *sarama.Config
	Input       *sarama.Config
	Producer    *sarama.Config
	InputGroup  string
	InputTopic  string
	OutputTopic string
	StateTopic  string
}

func NewConfiguartion(brokers []string, transactionalID string, namespace string) Configuration {
	newConfig := func() *sarama.Config {
		config := sarama.NewConfig()
		config.Version = sarama.V3_6_0_0
		return config
	}

	input := newConfig()
	input.Consumer.Offsets.Initial = sarama.OffsetOldest
	input.Consumer.IsolationLevel = sarama.ReadCommitted
	input.Consumer.Offsets.AutoCommit.Enable = false

	state := sarama.NewConfig()
	state.Version = sarama.V3_6_0_0
	state.Consumer.Return.Errors = true
	state.Consumer.Offsets.AutoCommit.Enable = false
	state.Consumer.Offsets.Initial = sarama.OffsetOldest
	state.Consumer.IsolationLevel = sarama.ReadCommitted

	producer := newConfig()
	producer.Net.MaxOpenRequests = 1
	producer.Producer.Idempotent = true
	// producer.Producer.Return.Errors = true
	// producer.Producer.Return.Successes = true
	producer.Producer.RequiredAcks = sarama.WaitForAll
	producer.Producer.Transaction.ID = transactionalID

	return Configuration{
		Brokers:     brokers,
		State:       state,
		Input:       input,
		Producer:    producer,
		InputGroup:  namespace + ".delivery-aggregation-group",
		InputTopic:  namespace + ".bid-events",
		OutputTopic: namespace + ".delivery-events",
		StateTopic:  namespace + ".delivery-state",
	}
}

func NewDeliveryAggregation(configuration Configuration) *DeliveryAggregation {
	group, err := sarama.NewConsumerGroup(configuration.Brokers, configuration.InputGroup, configuration.Input)
	if err != nil {
		log.Fatalf("Error NewConsumerGroupFromClient: %v", err)
	}

	return &DeliveryAggregation{
		group:         group,
		configuration: configuration,
	}
}

func (d *DeliveryAggregation) Run(ctx context.Context) {
	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := d.group.Consume(ctx, []string{d.configuration.InputTopic}, d); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}
			log.Fatalf("Error from consumer Topic: %s Error: %v", d.configuration.InputTopic, err)
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			return
		}
	}
}

func (d *DeliveryAggregation) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (d *DeliveryAggregation) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (d *DeliveryAggregation) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (err error) {
	txState := LoadState(d.configuration, claim.Partition())

	tx, err := NewStatefulEventProcessing(d.configuration, session, claim)
	if err != nil {
		log.Printf("NewStatefulEventProcessing failed: %+v", err)
		return err
	}
	defer tx.Close()

	ok := true
	for ok {
		ok, err = tx.ProcessNext(func(m *sarama.ConsumerMessage) (results []*sarama.ProducerMessage) {
			var message BidEventMessage
			if err = message.DecodeMessage(m); err != nil {
				log.Printf("DecodeMessage offset: %d, key: %s, Error: %s", m.Offset, m.Key, err)
			} else {
				// log.Printf("DecodeMessage offset: %d, key: %s valie: %v", m.Offset, m.Key, string(m.Value))
			}

			if state, event, err := txState.HandleBidEvent(message); err != nil {
				log.Printf("HandleBidEvent offset: %d, key: %s, Error: %s", m.Offset, m.Key, err)
			} else if event == nil && state == nil {
				// log.Printf("HandleBidEvent offset: %d, key: %s, Skipping", m.Offset, m.Key)
			} else {
				if event != nil {
					results = append(results, event.EncodeMessage(d.configuration.OutputTopic))
				}
				if state != nil {
					results = append(results, state.EncodeMessage(d.configuration.StateTopic))
				}
			}
			return
		})
	}
	return nil
}

type StateConsumer struct {
	client            sarama.Client
	topicConsumer     sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
	topic             string
	partition         int32
	lastOffset        int64
	isEOF             bool
}

func NewStateConsumer(configuraion Configuration, partition int32) *StateConsumer {
	client, err := sarama.NewClient(configuraion.Brokers, configuraion.State)
	if err != nil {
		log.Fatalf("NewStateConsumer NewClient %s", err)
	}

	topicConsumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalf("NewStateConsumer NewConsumerFromClient %s", err)
	}

	topic := configuraion.StateTopic
	partitionConsumer, err := topicConsumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("NewStateConsumer ConsumePartition %s", err)
	}

	return &StateConsumer{
		client:            client,
		topicConsumer:     topicConsumer,
		partitionConsumer: partitionConsumer,
		topic:             topic,
		partition:         partition,
	}
}

func (c *StateConsumer) Close() {
	if c.partitionConsumer != nil {
		c.partitionConsumer.Close()
	}
	if c.topicConsumer != nil {
		c.topicConsumer.Close()
	}
	if c.client != nil {
		c.client.Close()
	}
}

func (c *StateConsumer) NextMessage() (m *sarama.ConsumerMessage, err error) {
	if c.isEOF {
		return nil, nil
	}

	select {
	case m = <-c.partitionConsumer.Messages():
		if m != nil && m.Offset >= c.lastOffset {
			var newestOffset int64
			if newestOffset, err = c.client.GetOffset(c.topic, c.partition, sarama.OffsetNewest); err != nil {
				return
			}
			c.lastOffset = newestOffset - 1
			if m.Offset >= c.lastOffset {
				c.isEOF = true
			}
		}
		return
	case e := <-c.partitionConsumer.Errors():
		if e.Err.Error() == "EOF" {
			return
		}
		return nil, e
	}
}

func LoadState(configuraion Configuration, partition int32) StateMachine {
	stateConsumer := NewStateConsumer(configuraion, partition)
	defer stateConsumer.Close()
	return LoadStateFrom(stateConsumer)
}

func LoadStateFrom(c *StateConsumer) StateMachine {
	stateCount := 0
	state := NewStateMachine()
	for {
		stateCount++
		m, err := c.NextMessage()
		if err != nil {
			log.Fatalf("DeliveryAggregation ConsumeClaim consumeDeliveryState offset: %d, error: %s", m.Offset, err)
		}
		if m == nil {
			break
		}
		// log.Printf("LoadState count: %d last offset: %d", stateCount, m.Offset)
		var message TxStateMessage
		err = message.DecodeMessage(m)
		if err != nil {
			log.Printf("ConsumeClaim DecodeMessage offset: %d, error: %s", m.Offset, err)
		}
		state.LoadState(message)
	}
	log.Printf("LoadState completed")
	return state
}

type StatefulEventProcessing struct {
	configuration Configuration
	session       sarama.ConsumerGroupSession
	claim         sarama.ConsumerGroupClaim
	producer      sarama.AsyncProducer
}

func NewStatefulEventProcessing(configuraion Configuration, session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (result StatefulEventProcessing, err error) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_6_0_0
	config.Net.MaxOpenRequests = 1
	config.Producer.Idempotent = true
	// producer.Producer.Return.Errors = true
	// producer.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Transaction.ID = configuraion.Producer.Producer.Transaction.ID
	producer, err := sarama.NewAsyncProducer(configuraion.Brokers, config)
	if err == nil {
		result = StatefulEventProcessing{
			configuration: configuraion,
			session:       session,
			claim:         claim,
			producer:      producer,
		}
	}
	return
}

func (d *StatefulEventProcessing) GetProducer() (producer sarama.AsyncProducer, err error) {
	if d.producer != nil {
		if d.producer.TxnStatus()&sarama.ProducerTxnFlagInError != 0 {
			d.producer.Close()
			d.producer = nil
		} else {
			return d.producer, nil
		}
	}

	producer, err = sarama.NewAsyncProducer(d.configuration.Brokers, d.configuration.Producer)
	d.producer = producer
	return
}

func (c *StatefulEventProcessing) NextMessage() (m *sarama.ConsumerMessage) {
	select {
	case m = <-c.claim.Messages():
		if m == nil {
			// log.Printf("consumeBidEvent claim.Messages() == nil")
			return
		}
		return
	case <-c.session.Context().Done():
		// log.Printf("consumeBidEvent session.Context().Done()")
		return
	}
}

func (p *StatefulEventProcessing) ProcessNext(next func(m *sarama.ConsumerMessage) []*sarama.ProducerMessage) (ok bool, err error) {
	producer, err := p.GetProducer()
	if err != nil {
		log.Printf("getProducer failed: %+v", err)
		return false, err
	}

	m := p.NextMessage()
	if m == nil {
		// log.Printf("message channel was closed")
		return false, nil
	}

	// startTime := time.Now()

	// BeginTxn must be called before any messages.
	if err = producer.BeginTxn(); err != nil {
		log.Printf("Message consumer: unable to start transaction: %+v", err)
		return
	}

	for _, pm := range next(m) {
		producer.Input() <- pm
	}

	// You can add current message to this transaction
	if err = producer.AddMessageToTxn(m, p.configuration.InputGroup, nil); err != nil {
		log.Println("error on AddMessageToTxn")
		p.handleTxnError(m, err, func() error {
			return producer.AddMessageToTxn(m, p.configuration.InputGroup, nil)
		})
		return
	}

	// Commit producer transaction.
	if err = producer.CommitTxn(); err != nil {
		log.Println("error on CommitTxn")
		p.handleTxnError(m, err, func() error {
			return producer.CommitTxn()
		})
		return
	}

	// log.Printf("Message claimed [%s]: value = %s, timestamp = %v, topic = %s, partition = %d", time.Since(startTime), string(m.Value), m.Timestamp, m.Topic, m.Partition)

	return true, nil
}

func (p *StatefulEventProcessing) handleTxnError(message *sarama.ConsumerMessage, err error, defaulthandler func() error) {
	log.Printf("Message consumer: unable to process transaction: %+v", err)
	for {
		producer, err := p.GetProducer()
		if err != nil {
			log.Printf("GetProducer error: %+v", err)
			return
		}
		if producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
			// fatal error. need to recreate producer.
			log.Printf("Message consumer: producer is in a fatal state, need to recreate it")
			// reset current consumer offset to retry consume this record.
			p.session.ResetOffset(message.Topic, message.Partition, message.Offset, "")
			return
		}
		if producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
			err = producer.AbortTxn()
			if err != nil {
				log.Printf("Message consumer: unable to abort transaction: %+v", err)
				continue
			}
			// reset current consumer offset to retry consume this record.
			p.session.ResetOffset(message.Topic, message.Partition, message.Offset, "")
			return
		}
		log.Printf("Retry")
		// if not you can retry
		if err = defaulthandler(); err == nil {
			return
		}
	}
}

func (p *StatefulEventProcessing) Close() {
	if p.producer != nil {
		p.producer.Close()
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
