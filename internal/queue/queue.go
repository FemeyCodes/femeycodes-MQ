package queue

import (
	"container/heap"
	"math"
	"os"
	"sync"
	"time"

	"github.com/FemeyCodes/femeycodes-MQ/internal/message"
)

type Queue struct {
	name    string
	heap    *priorityHeap
	logFile *os.File
	dlq     *DeadLetterQueue
	mu      sync.RWMutex
}

type DeadLetterQueue struct {
	queue      *priorityHeap
	maxRetries int32
	backoff    time.Duration
	mu         sync.RWMutex
}

func NewQueue(name string, maxRetries int32, backoff time.Duration) *Queue {
	return &Queue{
		name:    name,
		heap:    NewPriorityHeap(),
		logFile: nil,
		dlq:     NewDeadLetterQueue(maxRetries, backoff),
	}
}

func NewDeadLetterQueue(maxRetires int32, backoff time.Duration) *DeadLetterQueue {
	return &DeadLetterQueue{
		queue:      NewPriorityHeap(),
		maxRetries: maxRetires,
		backoff:    backoff,
	}
}

func (q *Queue) Enqueue(payload []byte, priority int32, metadata map[string]string) *message.Message {
	msg := q.heap.Enqueue(payload, priority, metadata)
	return msg
}

func (q *Queue) Dequeue() *message.Message {
	msg := q.heap.Dequeue()
	return msg
}

func (q *Queue) HandleFailure(msg *message.Message) {
	q.mu.Lock()
	defer q.mu.Unlock()

	msg.RetryCount++
	if msg.RetryCount > q.dlq.maxRetries {
		q.dlq.mu.Lock()
		heap.Push(q.dlq.queue, msg)
		q.dlq.mu.Unlock()
		return
	}

	// Schedule retry with exponential backoff
	backoff := q.dlq.backoff * time.Duration(math.Pow(2, float64(msg.RetryCount)))
	time.AfterFunc(backoff, func() {
		q.mu.Lock()
		heap.Push(q.heap, msg)
		q.mu.Unlock()
	})
}

// Number of messages in DeadLetter Queue
func (dlq *DeadLetterQueue) Size() int32 {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()
	return int32(dlq.queue.Len())

}

func (dlq *DeadLetterQueue) RetrieveDLQMessages() []*message.Message {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	// Create a copy of the heap to avoid modifying the original and send it in priority order
	tempHeap := &priorityHeap{items: make([]*message.Message, len(dlq.queue.items))}
	copy(tempHeap.items, dlq.queue.items)

	messages := make([]*message.Message, 0, tempHeap.Len())
	for tempHeap.Len() > 0 {
		msg := heap.Pop(tempHeap).(*message.Message)
		messages = append(messages, msg)
	}
	return messages
}

// ClearDLQ removes all messages from the dead-letter queue.
func (dlq *DeadLetterQueue) ClearDLQ() {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	dlq.queue.items = make([]*message.Message, 0)
}
