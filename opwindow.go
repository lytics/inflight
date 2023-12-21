package inflight

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"
)

// OpWindow is a windowed, microbatching priority queue.
// Operations for the same ID and time window form a microbatch. Microbatches whose windows have passed are dequeued in FIFO order.
// OpWindow provides back-pressure for both depth (i.e., number of entries in queue) and width (i.e., number of entries in a microbatch).
// OpWindow is safe for concurrent use. Its zero value is not safe to use, use NewOpWindow().
type OpWindow struct {
	mu sync.Mutex
	q  list.List // *queueItem
	m  map[ID]*queueItem

	// These are selectable sync.Cond: use blocking read for Wait() and non-blocking write for Signal().
	queueHasItems chan struct{}
	queueHasSpace chan struct{}

	once sync.Once
	done chan struct{}

	depth      int
	width      int
	windowedBy time.Duration
}

// NewOpWindow creates a new OpWindow.
//
//	depth: maximum number of entries in a queue
//	width: maximum number of entries in a microbatch.
//	windowedBy: window size.
func NewOpWindow(depth, width int, windowedBy time.Duration) *OpWindow {
	q := &OpWindow{
		queueHasItems: make(chan struct{}, 1),
		queueHasSpace: make(chan struct{}, 1),
		done:          make(chan struct{}),
		depth:         depth,
		width:         width,
		windowedBy:    windowedBy,
		m:             make(map[ID]*queueItem),
	}
	q.q.Init()
	return q
}

// Close provides graceful shutdown: no new ops will be enqueued.
func (q *OpWindow) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.once.Do(func() {
		close(q.done)
		// HACK (2023-12) (mh): Set depth to zero so new entries are rejected.
		q.depth = 0
	})
}

// Enqueue op into queue, blocking until first of: op is enqueued, ID has hit max width, context is done, or queue is closed.
func (q *OpWindow) Enqueue(ctx context.Context, id ID, op *Op) error {
	q.mu.Lock() // locked on returns below

	for {
		item, ok := q.m[id]
		if ok {
			if len(item.OpSet.set) >= q.width {
				q.mu.Unlock()
				return ErrQueueSaturatedWidth
			}
			item.OpSet.append(op)
			q.mu.Unlock()
			return nil
		}

		if q.q.Len() >= q.depth {
			q.mu.Unlock()
			select {
			case <-ctx.Done():
				return fmt.Errorf("%w: %w", ErrQueueSaturatedDepth, ctx.Err())
			case <-q.done:
				return ErrQueueClosed
			case <-q.queueHasSpace:
				q.mu.Lock()
				continue
			}
		}

		item = &queueItem{
			ID:        id,
			ProcessAt: time.Now().Add(q.windowedBy),
			OpSet:     newOpSet(op),
		}
		q.m[id] = item
		q.q.PushBack(item)
		q.mu.Unlock()

		select {
		case q.queueHasItems <- struct{}{}:
		default:
		}

		return nil
	}
}

// Dequeue removes and returns the oldest OpSet whose window has passed from the queue,
// blocking until first of: OpSet is ready, context is canceled, or queue is closed.
func (q *OpWindow) Dequeue(ctx context.Context) (*OpSet, error) {
	q.mu.Lock() // unlocked on returns below

	var item *queueItem
	for item == nil {
		elem := q.q.Front()
		if elem == nil {
			q.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-q.done:
				return nil, ErrQueueClosed
			case <-q.queueHasItems:
				q.mu.Lock()
				continue
			}

		}
		item = q.q.Remove(elem).(*queueItem) // next caller will wait for a different item
	}

	waitFor := time.Until(item.ProcessAt)
	if waitFor > 0 {
		q.mu.Unlock() // allow others to add to OpQueue while we wait
		// NOTE (2023-12) (mh): Do we need to pool these?
		timer := time.NewTimer(waitFor)
		select {
		case <-q.done:
			// process right away
			timer.Stop()
		case <-timer.C:
		}
		q.mu.Lock()
	}

	ops := item.OpSet
	delete(q.m, item.ID)
	q.mu.Unlock()
	item = nil // gc

	select {
	case q.queueHasSpace <- struct{}{}:
	default:
	}
	return ops, nil
}

type queueItem struct {
	ID        ID
	ProcessAt time.Time
	OpSet     *OpSet
}
