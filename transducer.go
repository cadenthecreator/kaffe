package kaffe

import (
	"context"
	"errors"
	"log"

	"github.com/IBM/sarama"
)

type StateConsumer interface {
	NextMessage() (m *sarama.ConsumerMessage, err error)
}

type StateTransducer interface {
	LoadFrom(consumer StateConsumer, partition int32) error
	ProcessMessage(input *sarama.ConsumerMessage) (messages []*sarama.ProducerMessage, err error)
}

type SaramaTransducer struct {
	configuration Configuration
	group         sarama.ConsumerGroup
	transducer    StateTransducer
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

	state := newConfig()
	state.Consumer.Return.Errors = true
	state.Consumer.Offsets.AutoCommit.Enable = false
	state.Consumer.Offsets.Initial = sarama.OffsetOldest
	state.Consumer.IsolationLevel = sarama.ReadCommitted

	input := newConfig()
	input.Consumer.Offsets.Initial = sarama.OffsetOldest
	input.Consumer.IsolationLevel = sarama.ReadCommitted
	input.Consumer.Offsets.AutoCommit.Enable = false

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

func NewSaramaTransducer(configuration Configuration, transducer StateTransducer) *SaramaTransducer {
	group, err := sarama.NewConsumerGroup(configuration.Brokers, configuration.InputGroup, configuration.Input)
	if err != nil {
		log.Fatalf("Error NewConsumerGroupFromClient: %v", err)
	}

	return &SaramaTransducer{
		group:         group,
		configuration: configuration,
		transducer:    transducer,
	}
}

func (d *SaramaTransducer) Run(ctx context.Context) {
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

func (d *SaramaTransducer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (d *SaramaTransducer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (d *SaramaTransducer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (err error) {
	d.loadState(claim.Partition())

	tx, err := newClaimTransducer(d.configuration, d.transducer, session, claim)
	if err != nil {
		log.Printf("NewStatefulEventProcessing failed: %+v", err)
		return err
	}
	defer tx.Close()

	ok := true
	for ok {
		ok, err = tx.ProcessNext()
	}
	return nil
}

func (d *SaramaTransducer) loadState(partition int32) {
	stateConsumer := d.newStaeConsumer(partition)
	defer stateConsumer.Close()
	d.transducer.LoadFrom(stateConsumer, partition)
}

func (d *SaramaTransducer) newStaeConsumer(partition int32) *stateConsumer {
	client, err := sarama.NewClient(d.configuration.Brokers, d.configuration.State)
	if err != nil {
		log.Fatalf("NewStateConsumer NewClient %s", err)
	}

	topicConsumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalf("NewStateConsumer NewConsumerFromClient %s", err)
	}

	topic := d.configuration.StateTopic
	partitionConsumer, err := topicConsumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("NewStateConsumer ConsumePartition %s", err)
	}

	return &stateConsumer{
		client:            client,
		topicConsumer:     topicConsumer,
		partitionConsumer: partitionConsumer,
		topic:             topic,
		partition:         partition,
	}
}

type stateConsumer struct {
	client            sarama.Client
	topicConsumer     sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
	topic             string
	partition         int32
	lastOffset        int64
	isEOF             bool
}

func (c *stateConsumer) Close() {
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

func (c *stateConsumer) NextMessage() (m *sarama.ConsumerMessage, err error) {
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

type claimTransducer struct {
	configuration Configuration
	transducer    StateTransducer
	session       sarama.ConsumerGroupSession
	claim         sarama.ConsumerGroupClaim
	producer      sarama.AsyncProducer
}

func newClaimTransducer(configuraion Configuration, transducer StateTransducer, session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (result claimTransducer, err error) {
	producer, err := sarama.NewAsyncProducer(configuraion.Brokers, configuraion.Producer)
	if err == nil {
		result = claimTransducer{
			configuration: configuraion,
			transducer:    transducer,
			session:       session,
			claim:         claim,
			producer:      producer,
		}
	}
	return
}

func (c *claimTransducer) GetProducer() (producer sarama.AsyncProducer, err error) {
	if c.producer != nil {
		if c.producer.TxnStatus()&sarama.ProducerTxnFlagInError != 0 {
			c.producer.Close()
			c.producer = nil
		} else {
			return c.producer, nil
		}
	}

	producer, err = sarama.NewAsyncProducer(c.configuration.Brokers, c.configuration.Producer)
	c.producer = producer
	return
}

func (c *claimTransducer) NextMessage() (m *sarama.ConsumerMessage) {
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

func (c *claimTransducer) ProcessNext() (ok bool, err error) {
	producer, err := c.GetProducer()
	if err != nil {
		log.Printf("getProducer failed: %+v", err)
		return false, err
	}

	m := c.NextMessage()
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

	if output, err := c.transducer.ProcessMessage(m); err != nil {
		return false, err
	} else {
		for _, pm := range output {
			producer.Input() <- pm
		}
	}

	// You can add current message to this transaction
	if err = producer.AddMessageToTxn(m, c.configuration.InputGroup, nil); err != nil {
		log.Println("error on AddMessageToTxn")
		c.handleTxnError(m, err, func() error {
			return producer.AddMessageToTxn(m, c.configuration.InputGroup, nil)
		})
		return
	}

	// Commit producer transaction.
	if err = producer.CommitTxn(); err != nil {
		log.Println("error on CommitTxn")
		c.handleTxnError(m, err, func() error {
			return producer.CommitTxn()
		})
		return
	}

	// log.Printf("Message claimed [%s]: value = %s, timestamp = %v, topic = %s, partition = %d", time.Since(startTime), string(m.Value), m.Timestamp, m.Topic, m.Partition)

	return true, nil
}

func (c *claimTransducer) handleTxnError(message *sarama.ConsumerMessage, err error, defaulthandler func() error) {
	log.Printf("Message consumer: unable to process transaction: %+v", err)
	for {
		producer, err := c.GetProducer()
		if err != nil {
			log.Printf("GetProducer error: %+v", err)
			return
		}
		if producer.TxnStatus()&sarama.ProducerTxnFlagFatalError != 0 {
			// fatal error. need to recreate producer.
			log.Printf("Message consumer: producer is in a fatal state, need to recreate it")
			// reset current consumer offset to retry consume this record.
			c.session.ResetOffset(message.Topic, message.Partition, message.Offset, "")
			return
		}
		if producer.TxnStatus()&sarama.ProducerTxnFlagAbortableError != 0 {
			err = producer.AbortTxn()
			if err != nil {
				log.Printf("Message consumer: unable to abort transaction: %+v", err)
				continue
			}
			// reset current consumer offset to retry consume this record.
			c.session.ResetOffset(message.Topic, message.Partition, message.Offset, "")
			return
		}
		log.Printf("Retry")
		// if not you can retry
		if err = defaulthandler(); err == nil {
			return
		}
	}
}

func (c *claimTransducer) Close() {
	if c.producer != nil {
		c.producer.Close()
	}
}
