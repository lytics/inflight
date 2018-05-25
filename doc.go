/*
Package inflight provides primitives(data strutures) for managing inflight operations that
are being processed in a distributed system.

CallGroup spawns off a group of operations for each call to Add() and
calls the CallGroupCompletion func when the last operation have
completed.  The CallGroupCompletion func can be thought of as a finalizer where
one can gather errors and/or results from the function calls.

OpQueue is a thread-safe duplicate operation suppression queue, that combines
duplicate operations (queue entires) into sets that will be dequeued togather.
*/
package inflight
