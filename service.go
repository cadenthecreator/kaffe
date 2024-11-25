package kaffe

import (
	"log"

	"github.com/IBM/sarama"
)

type DeliveryAggregation struct {
	state       StateMachine
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
	d.state = NewStateMachine()
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
		var message TxStateMessage
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
	var message BidEventMessage
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
