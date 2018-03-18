package testutils

import "sync/atomic"

// AtomicInt provides an atomic int with built in increment
// decrement helpers for easy counting
type AtomicInt struct {
	Val int64 `json:"val"`
}

func (i *AtomicInt) Set(value int64) {
	atomic.StoreInt64(&(i.Val), value)
}

func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64(&(i.Val))
}

func (i *AtomicInt) Incr() {
	atomic.AddInt64(&(i.Val), 1)
}

func (i *AtomicInt) Decr() {
	atomic.AddInt64(&(i.Val), -1)
}

func (i *AtomicInt) IncrBy(by int) {
	atomic.AddInt64(&(i.Val), int64(by))
}

func (i *AtomicInt) DecrBy(by int) {
	byt := 0 - by
	atomic.AddInt64(&(i.Val), int64(byt))
}
