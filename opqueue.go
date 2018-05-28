package inflight

import (
	"container/list"
	"context"
	"fmt"
	"sync"
)

var (
	// ErrQueueSaturatedDepth is the error returned when the queue has reached
	// it's max queue depth
	ErrQueueSaturatedDepth = fmt.Errorf("queue is saturated (depth)")

	// ErrQueueSaturatedWidth is the error returned when a OpSet (aka a row) with
	// in the queue has reached it's max width.  This happens when one submits
	// many duplicate IDs.
	ErrQueueSaturatedWidth = fmt.Errorf("queue is saturated (width)")
)

// OpSet represents the set of Ops that have been merged in an OpQueue,
// It provides convenience functions for appending new Ops and for completing them.
type OpSet struct {
	set []*Op
}

func newOpSet() *OpSet {
	return &OpSet{
		set: []*Op{},
	}
}

func (os *OpSet) append(op *Op) {
	os.set = append(os.set, op)
}

// Ops get the list of ops in this set.
func (os *OpSet) Ops() []*Op {
	return os.set
}

// FinishAll a convenience func that calls finish on each Op in the set, passing the
// results or error to all the Ops in the OpSet.
//
// NOTE: The call group that owns this OP will not call it's finish function until all
// Ops are complete.  And one callgroup could be spread over multiple op sets or
// multiple op queues.
func (os *OpSet) FinishAll(err error, resp interface{}) {
	for _, op := range os.set {
		op.Finish(err, resp)
	}
}

// OpQueue is a thread-safe duplicate operation suppression queue, that combines
// duplicate operations (queue entires) into sets that will be dequeued together.

// For example, If you enqueue an item with a key that already exists, then that
// item will be appended to that key's set of items. Otherwise the item is
// inserted into the head of the list as a new item.

// On Dequeue a SET is returned of all items that share a key in the queue.
// It blocks on dequeue if the queue is empty, but returns an error if the
// queue is full during enqueue.
type OpQueue struct {
	cond *sync.Cond
	ctx  context.Context
	can  context.CancelFunc

	depth   int
	width   int
	q       *list.List
	entries map[ID]*OpSet
}

func NewOpQueue(depth, width int) *OpQueue {
	cond := sync.NewCond(&sync.Mutex{})
	myctx, can := context.WithCancel(context.Background())
	q := &OpQueue{
		cond:    cond,
		ctx:     myctx,
		can:     can,
		depth:   depth,
		width:   width,
		q:       list.New(),
		entries: map[ID]*OpSet{},
	}
	go func() {
		<-myctx.Done()
		q.cond.L.Lock()
		defer q.cond.L.Unlock()
		cond.Broadcast() // alert all dequeue calls that they should wake up and return.
	}()
	return q
}

// Close releases resources associated with this callgroup, by canceling the context.
// The owner of this OpQueue should either call Close or cancel the context, both are
// equivalent.
func (q *OpQueue) Close() {
	q.can()
}

// Len returns the number of uniq IDs in the queue, that is the depth of the queue.
func (q *OpQueue) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.q.Len()
}

// Enqueue add the op to the queue.  If the ID already exists then the Op
// is added to the existing OpSet for this ID, otherwise it's inserted as a new
// OpSet.
//
// Enqueue doesn't block if the queue if full, instead it returns a ErrQueueSaturated
// error.
func (q *OpQueue) Enqueue(id ID, op *Op) error {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	if q.q.Len() >= q.depth {
		return ErrQueueSaturatedDepth
	}

	set, ok := q.entries[id]
	if !ok {
		set = newOpSet()
		// This is a new item, so we need to insert it into the queue.
		q.enqueue(id)

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
	}

	if len(set.Ops()) >= q.width {
		return ErrQueueSaturatedWidth
	}

	set.append(op)
	q.entries[id] = set

	return nil
}

// Dequeue removes the oldest OpSet from the queue and returns it.
// Dequeue will block if the Queue is empty.  An Enqueue will wake the
// go routine up and it will continue on.
//
// If the OpQueue is closed, then Dequeue will return false
// for the second parameter.
func (q *OpQueue) Dequeue() (*OpSet, bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	for {
		if set, ok := q.dequeue(); ok {
			return set, true
		}

		select {
		case <-q.ctx.Done():
			return nil, false
		default:
		}
		// release the lock and wait until signaled.  On awake we'll require the lock.
		// After wait requires the lock we have to recheck the wait condition
		// (calling q.dequeue), because it's possible that someone else
		// drained the queue while, we were reacquiring the lock.
		q.cond.Wait()
		select {
		case <-q.ctx.Done():
			return nil, false
		default:
		}
	}
}

func (q *OpQueue) enqueue(id ID) {
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
