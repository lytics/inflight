package inflight

import (
	"container/list"
	"errors"
	"sync"
)

var (
	// ErrQueueSaturatedDepth is the error returned when the queue has reached
	// it's max queue depth
	ErrQueueSaturatedDepth = errors.New("queue is saturated (depth)")

	// ErrQueueSaturatedWidth is the error returned when a OpSet (aka a row) with
	// in the queue has reached it's max width.  This happens when one submits
	// many duplicate IDs.
	ErrQueueSaturatedWidth = errors.New("queue is saturated (width)")
	ErrQueueClosed         = errors.New("queue closed")
)

// OpQueue is a thread-safe duplicate operation suppression queue, that combines
// duplicate operations (queue entires) into sets that will be dequeued together.
//
// For example, If you enqueue an item with a key that already exists, then that
// item will be appended to that key's set of items. Otherwise the item is
// inserted into the head of the list as a new item.
//
// On Dequeue a SET is returned of all items that share a key in the queue.
// It blocks on dequeue if the queue is empty, but returns an error if the
// queue is full during enqueue.
type OpQueue struct {
	mu      sync.Mutex
	cond    sync.Cond
	depth   int
	width   int
	q       *list.List
	entries map[ID]*OpSet
	closed  bool
}

// NewOpQueue create a new OpQueue.
func NewOpQueue(depth, width int) *OpQueue {
	q := OpQueue{
		depth:   depth,
		width:   width,
		q:       list.New(),
		entries: map[ID]*OpSet{},
	}
	q.cond.L = &q.mu
	return &q
}

// Close releases resources associated with this callgroup, by canceling the context.
// The owner of this OpQueue should either call Close or cancel the context, both are
// equivalent.
func (q *OpQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast() // alert all dequeue calls that they should wake up and return.
}

// Len returns the number of uniq IDs in the queue, that is the depth of the queue.
func (q *OpQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.q.Len()
}

// Enqueue add the op to the queue.  If the ID already exists then the Op
// is added to the existing OpSet for this ID, otherwise it's inserted as a new
// OpSet.
//
// Enqueue doesn't block if the queue if full, instead it returns a ErrQueueSaturated
// error.
func (q *OpQueue) Enqueue(id ID, op *Op) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}
	if q.q.Len() >= q.depth {
		return ErrQueueSaturatedDepth
	}

	set, ok := q.entries[id]
	if !ok {
		// This is a new item, so we need to insert it into the queue.
		q.newEntry(id, op)

		// Signal one waiting go routine to wake up and Dequeue
		// I believe we only need to signal if we enqueue a new item.
		// Consider the following possible states the queue could be in  :
		//   1. if no one is currently waiting in Dequeue, the signal isn't
		//      needed and all items will be dequeued on the next call to
		//      Dequeue.
		//   2. One or Many go-routines are waiting in Dequeue because it's
		//      empty, and calling Signal will wake up one.  Which will dequeue
		//      the item and return.
		//   3. At most One go-routine is in the act of Dequeueing existing items
		//      from the queue (i.e. only one can have the lock and be in the "if OK"
		//      condition within the forloop in Dequeue).  In which cause the signal
		//      is ignored and after returning we return to condition (1) above.
		// Note signaled waiting go-routines will not be able the acquire
		// the condition lock until this method call returns, finishing
		// its append of the new operation.
		q.cond.Signal()
		return nil
	}
	if len(set.Ops()) >= q.width {
		return ErrQueueSaturatedWidth
	}

	// Q (2023-11) (mh): Why don't we signal here?
	set.append(op)
	return nil
}

// Dequeue removes the oldest OpSet from the queue and returns it.
// Dequeue will block if the Queue is empty.  An Enqueue will wake the
// go routine up and it will continue on.
//
// If the OpQueue is closed, then Dequeue will return false
// for the second parameter.
func (q *OpQueue) Dequeue() (*OpSet, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		if set, ok := q.dequeue(); ok {
			return set, true
		}
		if q.closed {
			return nil, false
		}

		// release the lock and wait until signaled.  On awake we'll acquire the lock.
		// After wait acquires the lock we have to recheck the wait condition,
		// because it's possible that someone else
		// drained the queue while, we were reacquiring the lock.
		q.cond.Wait()
	}
}

func (q *OpQueue) newEntry(id ID, op *Op) {
	set := newOpSet(op)
	q.entries[id] = set

	q.q.PushBack(id)
}

func (q *OpQueue) dequeue() (*OpSet, bool) {
	elem := q.q.Front()
	if elem == nil {
		return nil, false
	}
	idt := q.q.Remove(elem)
	id := idt.(ID)

	set, ok := q.entries[id]
	if !ok {
		panic("invariant broken: we dequeued a value that isn't in the map")
	}
	delete(q.entries, id)
	return set, true
}
