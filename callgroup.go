package inflight

import (
	"sync"
)

// ID is unique id.
type ID uint64

// CallGroup spawns off a group of operations for each call to Add() and
// calls the CallGroupCompletion func when the last operation have
// completed.  The CallGroupCompletion func can be thought of as a finalizer where
// one can gather errors and/or results from the function calls.
//
// Call Add for all our inflight tasks before calling the first
// call to Finish.  Once the last task finishes and the CallGroupCompletion
// is triggered, all future calls to Add will be ignored and orphaned.
//
type CallGroup struct {
	mu sync.Mutex

	cgcOnce             sync.Once
	callGroupCompletion CallGroupCompletion

	outstandingOps map[ID]*Op
	finalState     map[ID]*Response
}

// NewCallGroup return a new CallGroup.
// Takes a CallGroupCompletion func as an argument, which will be called when the last Op in
// the CallGroup has called Finish.
//
// In a way a CallGroup is like a Mapper-Reducer in other framworks, with
// the Ops being mapped out to workers and the CallGroupCompletion being the reducer step.
func NewCallGroup(cgc CallGroupCompletion) *CallGroup {
	return &CallGroup{
		outstandingOps:      map[ID]*Op{},
		finalState:          map[ID]*Response{},
		callGroupCompletion: cgc,
		cgcOnce:             sync.Once{},
	}
}

// Add a op to message to callgroup.
func (cg *CallGroup) Add(k uint64, msg interface{}) *Op {
	key := ID(k)

	op := &Op{
		cg:  cg,
		Key: key,
		Msg: msg,
	}

	cg.mu.Lock()
	defer cg.mu.Unlock()

	cg.outstandingOps[key] = op

	return op
}

// Used to by the package to extract the active ops for this callgroup.
func (cg *CallGroup) ops() map[ID]*Op {
	return cg.outstandingOps
}

func (cg *CallGroup) done() {
	if len(cg.outstandingOps) > 0 {
		return
	}

	cg.cgcOnce.Do(func() {
		//callGroupCompletion should never be nil, so let it panic if it is.
		cg.callGroupCompletion(cg.finalState)
	})
}

// CallGroupCompletion is the reducer function for a callgroup, its called once all
// Ops in the callgroup have called Finished and the final state is passed to this
// function.
type CallGroupCompletion func(finalState map[ID]*Response)

// Op represents one inflight operaton or message.  When this Op's Finish func is called
// the results for this Op will be added to the finalState.  When all Ops in the
// callgroup have called Finish, then the CallGroup's CallGroupCompletion func will be
// called with the final state for all Ops.
type Op struct {
	cg  *CallGroup
	Key ID
	Msg interface{}
}

// Finish this op.
func (o *Op) Finish(err error, resp interface{}) {
	o.cg.mu.Lock()
	defer o.cg.mu.Unlock()

	if err != nil {
		o.cg.finalState[o.Key] = &Response{Op: o, Err: err}
	} else {
		o.cg.finalState[o.Key] = &Response{Op: o, Result: resp}
	}
	delete(o.cg.outstandingOps, o.Key)

	o.cg.done()
}

// Response for an op.
type Response struct {
	Op     *Op
	Err    error
	Result interface{}
}
