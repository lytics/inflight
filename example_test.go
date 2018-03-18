package inflight_test

import (
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/lytics/inflight/testutils"
	"github.com/lytics/lio/src/lib/inflight"
)

//TestExample1 uses the callgroup to do a concurrently map reduce, by spliting and
// parsing an array of strings to calculate the subtotals for each entry.
// Then in the Complete func, the subtotals are reduced into a total.
func TestExample1(t *testing.T) {
	t.Parallel()
	data := []string{"1:2", "5:6:7", "1:2", "5:6:7"}
	total := testutils.AtomicInt{}
	wg := sync.WaitGroup{}
	wg.Add(1)
	cg := inflight.NewCallGroup(func(results map[inflight.ID]*inflight.Response) {
		for _, res := range results {
			subtotal := res.Result.(int)
			total.IncrBy(subtotal)
		}
		wg.Done()
	})

	startingLine := sync.WaitGroup{}
	startingLine.Add(1) // block all go routines until the loop has finished spinning them up.  Otherwise we have a race.
	//Spawn off the workers.
	for id, entry := range data {
		op := cg.Add(uint64(id), entry)
		go func(op *inflight.Op) {
			startingLine.Wait() //wait here until signaled to start.
			str := op.Msg.(string)
			subtotal := 0
			for _, val := range strings.Split(str, ":") {
				i, _ := strconv.ParseInt(val, 10, 64)
				subtotal += int(i)
			}
			op.Finish(nil, subtotal)
		}(op)
	}
	startingLine.Done() // drop the checkered flag and signal all the workers to begin.

	//wait for the completion function to finish.
	wg.Wait()
	totalVal := total.Get()
	if totalVal != 42 {
		// total == (1 + 2) + (5 + 6 + 7) + (1 + 2) + (5 + 6 + 7) == 42
		t.Fatalf("total not equal 42, got:%v", totalVal)
	}
}
