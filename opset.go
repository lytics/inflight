package inflight

// OpSet represents the set of Ops that have been merged in an OpQueue,
// It provides convenience functions for appending new Ops and for completing them.
type OpSet struct {
	set []*Op
}

func newOpSet(op *Op) *OpSet {
	return &OpSet{
		set: []*Op{op},
	}
}

func appendOp(ops []*Op, op *Op) []*Op {
	return append(ops, op)
}
func (os *OpSet) mergeWith(fn func([]*Op, *Op) []*Op, op *Op) {
	os.set = fn(os.set, op)
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
