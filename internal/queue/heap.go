package queue

import (
	"container/heap"
	"sync"
	"time"

	"github.com/FemeyCodes/femeycodes-MQ/internal/message"
	"github.com/google/uuid"
)

type priorityHeap struct {
	mu    sync.RWMutex
	items []*message.Message
}

func NewPriorityHeap() *priorityHeap {
	return &priorityHeap{
		items: make([]*message.Message, 0),
	}
}

func (p *priorityHeap) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.items)
}

func (p *priorityHeap) Less(i, j int) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.items[i].Priority == p.items[j].Priority {
		return p.items[i].Timestamp.Before(p.items[j].Timestamp)
	}

	return p.items[i].Priority < p.items[j].Priority
}

func (p *priorityHeap) Swap(i, j int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.items[i], p.items[j] = p.items[j], p.items[i]
}

func (p *priorityHeap) Push(val interface{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.items = append(p.items, val.(*message.Message))
}

func (p *priorityHeap) Pop() interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := len(p.items)
	if n == 0 {
		return nil
	}
	item := p.items[n-1]
	p.items = p.items[:n-1]
	return item
}

func (p *priorityHeap) Enqueue(payload []byte, priority int32, metadata map[string]string) *message.Message {
	msg := &message.Message{
		ID:        uuid.New().String(),
		Priority:  priority,
		Payload:   payload,
		Metadata:  metadata,
		Timestamp: time.Now(),
	}

	p.mu.Lock()
	heap.Push(p, msg)
	p.mu.Unlock()

	return msg
}

func (p *priorityHeap) Dequeue() *message.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.items) == 0 {
		return nil
	}
	return heap.Pop(p).(*message.Message)
}
