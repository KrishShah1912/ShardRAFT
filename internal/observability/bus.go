package observability

import (
	"sync"
	"time"
)

const (
	DefaultHistorySize    = 2048
	DefaultSubscriberSize = 256
)

type Subscriber struct {
	ID     uint64
	Events <-chan Event
}

type subscriber struct {
	ch chan Event
}

type Bus struct {
	mu sync.RWMutex

	nextEventID      uint64
	nextSubscriberID uint64

	history     []Event
	historySize int

	subscribers map[uint64]*subscriber
}

func NewBus(historySize int) *Bus {
	if historySize <= 0 {
		historySize = DefaultHistorySize
	}

	return &Bus{
		historySize: historySize,
		history:     make([]Event, 0, historySize),
		subscribers: make(map[uint64]*subscriber),
	}
}

func (b *Bus) Publish(event Event) Event {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextEventID++

	event.ID = b.nextEventID

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	event.Data = cloneData(event.Data)

	if len(b.history) == b.historySize {
		copy(b.history, b.history[1:])
		b.history[len(b.history)-1] = cloneEvent(event)
	} else {
		b.history = append(b.history, cloneEvent(event))
	}

	for _, sub := range b.subscribers {
		select {
		case sub.ch <- cloneEvent(event):
		default:
			// Observability must never block the database.
			// Slow subscribers can reconcile from authoritative
			// state or reconnect using event history.
		}
	}

	return cloneEvent(event)
}

func (b *Bus) Subscribe(bufferSize int) Subscriber {
	if bufferSize <= 0 {
		bufferSize = DefaultSubscriberSize
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextSubscriberID++

	id := b.nextSubscriberID
	ch := make(chan Event, bufferSize)

	b.subscribers[id] = &subscriber{
		ch: ch,
	}

	return Subscriber{
		ID:     id,
		Events: ch,
	}
}

func (b *Bus) Unsubscribe(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	sub, ok := b.subscribers[id]
	if !ok {
		return
	}

	delete(b.subscribers, id)
	close(sub.ch)
}

func (b *Bus) History() []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]Event, len(b.history))

	for i, event := range b.history {
		result[i] = cloneEvent(event)
	}

	return result
}

func (b *Bus) EventsAfter(id uint64) []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]Event, 0)

	for _, event := range b.history {
		if event.ID > id {
			result = append(result, cloneEvent(event))
		}
	}

	return result
}

func (b *Bus) LatestEventID() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.nextEventID
}

func (b *Bus) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.subscribers)
}

func cloneEvent(event Event) Event {
	event.Data = cloneData(event.Data)
	return event
}

func cloneData(data map[string]any) map[string]any {
	if data == nil {
		return nil
	}

	result := make(map[string]any, len(data))

	for key, value := range data {
		result[key] = value
	}

	return result
}
