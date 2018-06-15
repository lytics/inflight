package inflight

import (
	"context"
	"fmt"
	"sync"
)

type Wg struct {
	cond   *sync.Cond
	closed bool

	waiters int
}

func NewWg() *Wg {
	cond := sync.NewCond(&sync.Mutex{})
	return &Wg{
		cond:   cond,
		closed: false,
	}
}

var ErrClosed = fmt.Errorf("closed")
var ErrNegWg = fmt.Errorf("neg wg")

func (wg *Wg) Inc() error {
	wg.cond.L.Lock()
	defer wg.cond.L.Unlock()

	if wg.closed == true {
		return ErrClosed
	}
	wg.waiters++
	return nil
}

func (wg *Wg) Dec() error {
	wg.cond.L.Lock()
	defer wg.cond.L.Unlock()
	wg.waiters--
	if wg.waiters < 0 {
		wg.cond.Signal()
		return ErrNegWg
	}
	wg.cond.Signal()
	return nil
}

func (wg *Wg) Wait(ctx context.Context) error {
	exited := make(chan struct{})
	defer close(exited)

	wg.cond.L.Lock()
	defer wg.cond.L.Unlock()

	go func() {
		select {
		case <-ctx.Done():
		case <-exited:
			return
		}
		wg.cond.L.Lock()
		defer wg.cond.L.Unlock()
		wg.cond.Signal() // after the context is Done, we'll signal the Wait loop so it can wake up and check the ctx
	}()

	wg.closed = true
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
