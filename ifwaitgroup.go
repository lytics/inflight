package inflight

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type atomicBool struct {
	Flag int32
}

func (b *atomicBool) Set(value bool) {
	var i int32 = 0
	if value {
		i = 1
	}
	atomic.StoreInt32(&(b.Flag), int32(i))
}

func (b *atomicBool) Get() bool {
	if atomic.LoadInt32(&(b.Flag)) != 0 {
		return true
	}
	return false
}

type Wg struct {
	cond   *sync.Cond
	closed *atomicBool

	waiters int
}

func NewWg() *Wg {
	cond := sync.NewCond(&sync.Mutex{})
	return &Wg{
		cond:   cond,
		closed: &atomicBool{},
	}
}

var ErrClosed = fmt.Errorf("closed")
var ErrNegWg = fmt.Errorf("neg wg")

func (wg *Wg) Inc() error {
	if wg.closed.Get() == true {
		return ErrClosed
	}

	wg.cond.L.Lock()
	defer wg.cond.L.Unlock()
	wg.waiters++
	wg.cond.Signal()
	return nil
}

func (wg *Wg) Dec() error {
	wg.cond.L.Lock()
	defer wg.cond.L.Unlock()
	wg.waiters--
	//what if this is negative??
	wg.cond.Signal()
	return nil
}

func (wg *Wg) Wait(ctx context.Context) error {
	wg.cond.L.Lock()
	defer wg.cond.L.Unlock()

	go func() {
		<-ctx.Done()
		wg.cond.L.Lock()
		defer wg.cond.L.Unlock()
		wg.cond.Signal() // after the context is Done, we'll signal the Wait loop so it can wake up and check the ctx
	}()

	for {
		if wg.waiters == 0 {
			return nil
		} else if wg.waiters < 0 {
			return ErrNegWg
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			//release the lock and wait until signaled.  On awake we'll require the lock.
			// After wait requires the lock we have to recheck the wait condition
			// (calling wg.waiters).
			wg.cond.Wait()
		}
	}
}
