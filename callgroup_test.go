package inflight

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCompletion(t *testing.T) {
	t.Parallel()
	completed := 0
	reslen := 0
	cg := NewCallGroup(func(finalState map[ID]*Response) {
		completed++
		reslen += len(finalState)
	})

	now := time.Now()

	op1 := cg.Add(1, &tsMsg{123, now})
	op2 := cg.Add(2, &tsMsg{123, now})

	assert.Equal(t, 0, completed)
	assert.Equal(t, 0, reslen)
	op1.Finish(nil, nil)
	assert.Equal(t, 0, completed)
	assert.Equal(t, 0, reslen)
	op2.Finish(nil, nil)
	assert.Equal(t, 1, completed)
	assert.Equal(t, 2, reslen)
}

func TestConcurrentDone(t *testing.T) {
	runtime.GOMAXPROCS(16)
	t.Parallel()
	completed := 0
	reslen := 0
	cg := NewCallGroup(func(finalState map[ID]*Response) {
		completed++
		reslen += len(finalState)
	})

	ops := []*Op{}
	now := time.Now()

	for i := 0; i < 1000; i++ {
		ops = append(ops, cg.Add(uint64(i), &tsMsg{123, now}))
	}

	wgend := sync.WaitGroup{}
	wgstart := sync.WaitGroup{}
	wgstart.Add(1)

	for i := 0; i < 1000; i++ {
		wgend.Add(1)
		go func(id int) {
			defer wgend.Done()
			wgstart.Wait() //block until the testcase signals all go routines to fire at once.
			ops[id].Finish(nil, nil)
		}(i)
	}
	wgstart.Done() //start all go routines at the same time.
	wgend.Wait()

	assert.Equal(t, 1, completed)
	assert.Equal(t, 1000, reslen)
}

type tsMsg struct {
	ID   uint64
	Time time.Time
}
