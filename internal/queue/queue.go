package queue

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"encoding/gob"
	"math"
	"os"
	"sync"
	"time"

	"github.com/FemeyCodes/femeycodes-MQ/internal/message"
)

type Queue struct {
	name         string
	heap         *priorityHeap
	dlq          *DeadLetterQueue
	mu           sync.RWMutex
	logFile      *os.File
	logPath      string
	snapshotPath string
}

type DeadLetterQueue struct {
	queue      *priorityHeap
	maxRetries int32
	backoff    time.Duration
	mu         sync.RWMutex
}

func NewQueue(name string, maxRetries int32, backoff time.Duration) *Queue {
	logPath := name + ".log"
	snapshotPath := name + ".snapshot"
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic("Failed to open Log File: " + err.Error())
	}
	q := &Queue{
		name:         name,
		heap:         NewPriorityHeap(),
		logFile:      logFile,
		dlq:          NewDeadLetterQueue(maxRetries, backoff),
		logPath:      logPath,
		snapshotPath: snapshotPath,
	}

	if err := q.recover(); err != nil {
		panic("Failed to recover queue: " + err.Error())
	}

	//Start Periodic SnapShotting
	go q.startSnapShotting()

	return q
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
	err := q.appendToLog("enqueue", msg)
	if err != nil {
		panic("failed to append to log: " + err.Error())
	}
	return msg
}

func (q *Queue) Dequeue() *message.Message {
	msg := q.heap.Dequeue()
	err := q.appendToLog("dequeue", msg)
	if err != nil {
		panic("failed to append to log: " + err.Error())
	}
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

		if err := q.appendToLog("dlq", msg); err != nil {
			panic("failed to append to log: " + err.Error())
		}

		return
	}

	// Schedule retry with exponential backoff
	backoff := q.dlq.backoff * time.Duration(math.Pow(2, float64(msg.RetryCount)))
	time.AfterFunc(backoff, func() {
		q.mu.Lock()
		heap.Push(q.heap, msg)
		q.mu.Unlock()

		if err := q.appendToLog("retry", msg); err != nil {
			panic("failed to append to log: " + err.Error())
		}
	})

	if err := q.appendToLog("failure", msg); err != nil {
		panic("failed to append to log: " + err.Error())
	}
}

func (q *Queue) appendToLog(op string, message *message.Message) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(op)
	if err != nil {
		return err
	}

	err = enc.Encode(message)
	if err != nil {
		return err
	}

	data := buf.Bytes()
	length := uint32(len(data))
	lengthBuf := make([]byte, 4)
	binary.BigEndian.AppendUint32(lengthBuf, length)

	if _, err := q.logFile.Write(lengthBuf); err != nil {
		return err
	}

	if _, err := q.logFile.Write(data); err != nil {
		return err
	}

	return q.logFile.Sync()
}

func (q *Queue) takeSnapshot() error {
	q.mu.RLock()
	defer q.mu.RUnlock()

	tempFile, err := os.CreateTemp("", "snapshot-*.tmp")
	if err != nil {
		return err
	}
	defer tempFile.Close()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	messages := make([]*message.Message, 0, q.heap.Len())
	tempHeap := &priorityHeap{items: make([]*message.Message, len(q.heap.items))}
	copy(tempHeap.items, q.heap.items)
	for tempHeap.Len() > 0 {
		msg := heap.Pop(tempHeap).(*message.Message)
		messages = append(messages, msg)
	}

	err = enc.Encode(messages)
	if err != nil {
		return err
	}

	_, err = tempFile.Write(buf.Bytes())
	if err != nil {
		return err
	}

	err = tempFile.Sync()
	if err != nil {
		return err
	}

	return os.Rename(tempFile.Name(), q.snapshotPath)

}

func (q *Queue) startSnapShotting() error {

	return nil
}

func (q *Queue) recover() error {
	return nil
}

func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.logFile.Close()
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
