package inflight

import (
	"runtime"
	"sync"
	"testing"

	"github.com/bmizerany/assert"
)

func TestCompletion(t *testing.T) {
	t.Parallel()
	completed := 0
	reslen := 0
	cg := NewCallGroup(func(finalState map[ID]*Response) {
		completed++
		reslen += len(finalState)
	})

	op1 := cg.Add(1, &tsMsg{123, 5, "user", 1234567})
	op2 := cg.Add(2, &tsMsg{123, 5, "user", 2222222})

	assert.T(t, completed == 0)
	assert.T(t, reslen == 0)
	op1.Finish(nil, nil)
	assert.T(t, completed == 0)
	assert.T(t, reslen == 0)
	op2.Finish(nil, nil)
	assert.T(t, completed == 1)
	assert.T(t, reslen == 2)
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
	for i := 0; i < 1000; i++ {
		ops = append(ops, cg.Add(uint64(i), &tsMsg{123, 5, "user", uint64(i)}))
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

	assert.T(t, completed == 1)
	assert.T(t, reslen == 1000)
}

type tsMsg struct {
	Aid    int
	Gen    int
	Table  string
	RefsID uint64
}
