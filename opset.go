package inflight

import "time"

// OpSet represents the set of Ops that have been merged in an OpQueue,
// It provides convenience functions for appending new Ops and for completing them.
type OpSet struct {
	set []*Op
	// used by the opWindow determine when it's ok to dequeue
	enqueuedAt time.Time
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
