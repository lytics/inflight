package inflight

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"
)

// OpWindow is a thread-safe duplicate operation suppression queue,
// that combines duplicate operations (queue entires) into sets
// that will be dequeued together.
//
// For example, if you enqueue an item with a key that already exists,
// then that item will be appended to that key's set of items. Otherwise
// the item is inserted into the back of the list as a new item.
//
// On Dequeue the oldest OpSet is returned, containing all items that share a key in the
// queue. It blocks on dequeue if the queue is empty, but returns an
// error if the queue is full during enqueue.
type OpWindow struct {
	mu        sync.Mutex
	emptyCond sync.Cond
	fullCond  sync.Cond

	once sync.Once
	done chan struct{}

	depth      int
	width      int
	windowedBy time.Duration

	q *list.List // *queueItem
	m map[ID]*queueItem
}

// NewOpWindow create a new OpWindow.
func NewOpWindow(depth, width int, windowedBy time.Duration) *OpWindow {
	q := &OpWindow{
		done:       make(chan struct{}),
		depth:      depth,
		width:      width,
		q:          list.New(),
		m:          make(map[ID]*queueItem),
		windowedBy: windowedBy,
	}
	q.emptyCond.L = &q.mu
	q.fullCond.L = &q.mu
	return q
}

// Close provides graceful shutdown: no new ops can be enqueued and,
// once drained, De
func (q *OpWindow) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.once.Do(func() {
		close(q.done)
		q.windowedBy = 0 // turn off windowing so everything is dequeue
		// alert all dequeue calls that they should wake up and return.
		q.emptyCond.Broadcast()
		q.fullCond.Broadcast()
	})
}

// Len returns the number of uniq IDs in the queue, that is the depth of the
// queue.
func (q *OpWindow) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.q.Len()
}

// Enqueue add the op to the queue.
// If the ID already exists then the Op is added to the existing OpSet for this ID.
// Otherwise if  it's inserted as
// a new OpSet.
//
// Enqueue doesn't block if the queue if full, instead it returns a
// ErrQueueSaturated error.
func (q *OpWindow) Enqueue(ctx context.Context, id ID, op *Op) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	select {
	case <-q.done:
		return ErrQueueClosed
	default:
	}

	item, ok := q.m[id]
	if !ok {
		for q.q.Len() >= q.depth {
			select {
			case <-ctx.Done():
				return fmt.Errorf("%w: %w", ErrQueueSaturatedDepth, ctx.Err())
			case <-q.done:
				return ErrQueueClosed
			default:
				q.fullCond.Wait()
			}
		}
		// This is a new item, so we need to insert it into the queue.

		item := &queueItem{
			ID:        id,
			ProcessAt: time.Now().Add(q.windowedBy),
			OpSet:     newOpSet(op),
		}
		q.m[id] = item
		q.q.PushBack(item)

		q.emptyCond.Signal()
		return nil
	}
	if len(item.OpSet.set) >= q.width {
		return ErrQueueSaturatedWidth
	}

	item.OpSet.append(op)
	return nil
}

// Dequeue removes the oldest OpSet from the queue and returns it.
// Dequeue will block if the Queue is empty.  An Enqueue will wake the
// go routine up and it will continue on.
//
// If the OpWindow is closed, then Dequeue will return false
// for the second parameter.
func (q *OpWindow) Dequeue(ctx context.Context) (*OpSet, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var item *queueItem
	for item == nil {
		elem := q.q.Front()
		if elem == nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-q.done:
				return nil, ErrQueueClosed
			default:
				q.emptyCond.Wait()
				continue
			}

		}
		item = q.q.Remove(elem).(*queueItem) // next caller will wait for a different item
	}

	waitFor := time.Until(item.ProcessAt)
	if waitFor > 0 {
		q.mu.Unlock() // allow others to add to OpQueue while we wait
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
	item = nil // gc

	q.fullCond.Signal()
	return ops, nil
}

type queueItem struct {
	ID        ID
	ProcessAt time.Time
	OpSet     *OpSet
}
