package flow

import (
	"fmt"
	"sync"

	"github.com/BaritoLog/barito-flow/flow/types"
	"github.com/BaritoLog/barito-flow/prome"
	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
)

const (
	RetrieveMessageFailedError = errkit.Error("Retrieve message failed")

	// DefaultNumProcessWorkers is the fallback number of goroutines draining
	// and processing messages per topic worker, used when no explicit count
	// is provided (e.g. via BARITO_CONSUMER_NUM_PROCESS_WORKERS). Messages
	// are routed to a worker by partition number, so a given partition is
	// always handled by the same goroutine (preserving in-order
	// processing/offset marking per partition) while different partitions
	// can be processed concurrently.
	DefaultNumProcessWorkers = 5
)

type consumerWorker struct {
	name               string
	isStart            bool
	consumer           types.ClusterConsumer
	onErrorFunc        func(error)
	onSuccessFunc      func(*sarama.ConsumerMessage)
	onNotificationFunc func(*types.Notification)
	stop               chan struct{}
	stopOnce           sync.Once
	lastMessage        *sarama.ConsumerMessage
	numProcessWorkers  int
}

func NewConsumerWorker(name string, consumer types.ClusterConsumer, numProcessWorkers int) types.ConsumerWorker {
	if numProcessWorkers <= 0 {
		numProcessWorkers = DefaultNumProcessWorkers
	}

	return &consumerWorker{
		name:              name,
		consumer:          consumer,
		stop:              make(chan struct{}),
		numProcessWorkers: numProcessWorkers,
	}
}

func (w *consumerWorker) Start() {
	log.Warnf("Start worker '%s'", w.name)

	go w.loopErrors()
	go w.loopNotification()
	w.startProcessing()
}

func (w *consumerWorker) Stop() {
	if w.consumer != nil {
		w.consumer.Close()
	}

	w.stopOnce.Do(func() { close(w.stop) })
}

func (w *consumerWorker) Halt() {
	w.stopOnce.Do(func() { close(w.stop) })
	log.Warnf("Halt worker '%s'", w.name)
}

func (w *consumerWorker) IsStart() bool {
	return w.isStart
}

func (w *consumerWorker) OnError(f func(error)) {
	w.onErrorFunc = f
}

func (w *consumerWorker) OnSuccess(f func(*sarama.ConsumerMessage)) {
	w.onSuccessFunc = f
}

func (w *consumerWorker) OnNotification(f func(*types.Notification)) {
	w.onNotificationFunc = f
}

func (w *consumerWorker) OnConsumerFlush() error {
	log.Warn("OnConsumerFlush")
	err := w.consumer.CommitOffsets()
	if err != nil {
		log.Error(fmt.Errorf("Commit offset failed: %s", err))
	}
	return err
}

// startProcessing fans out messages to numProcessWorkers goroutines, keyed by
// partition. Routing by partition (rather than free-for-all) guarantees that
// messages belonging to the same partition are always processed by the same
// goroutine and therefore stay in order, which keeps offset marking safe.
func (w *consumerWorker) startProcessing() {
	w.isStart = true

	partitionChans := make([]chan *sarama.ConsumerMessage, w.numProcessWorkers)
	for i := range partitionChans {
		partitionChans[i] = make(chan *sarama.ConsumerMessage)
		go w.loopProcess(partitionChans[i])
	}

	go w.loopDispatch(partitionChans)
}

func (w *consumerWorker) loopDispatch(partitionChans []chan *sarama.ConsumerMessage) {
	defer func() { w.isStart = false }()

	for {
		select {
		case message, ok := <-w.consumer.Messages():
			if !ok {
				continue
			}
			target := partitionChans[int(message.Partition)%len(partitionChans)]
			select {
			case target <- message:
			case <-w.stop:
				return
			}
		case <-w.stop:
			return
		}
	}
}

func (w *consumerWorker) loopProcess(messages <-chan *sarama.ConsumerMessage) {
	for {
		select {
		case message := <-messages:
			prome.IncreaseKafkaMessagesIncoming(message.Topic)
			w.fireSuccess(message)
			w.consumer.MarkOffset(message, "")
		case <-w.stop:
			return
		}
	}
}

func (w *consumerWorker) loopNotification() {
	for notification := range w.consumer.Notifications() {
		w.fireNotification(notification)
	}
}

func (w *consumerWorker) loopErrors() {
	for err := range w.consumer.Errors() {
		w.fireError(errkit.Concat(RetrieveMessageFailedError, err))
	}
}

func (w *consumerWorker) fireSuccess(message *sarama.ConsumerMessage) {
	if w.onSuccessFunc != nil {
		w.onSuccessFunc(message)
	}
}

func (w *consumerWorker) fireError(err error) {
	if w.onErrorFunc != nil {
		w.onErrorFunc(err)
	}
}

func (w *consumerWorker) fireNotification(notification *types.Notification) {
	if w.onNotificationFunc != nil {
		w.onNotificationFunc(notification)
	}
}
