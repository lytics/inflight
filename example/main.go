package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/lytics/inflight"
)

func main() {
	data := []string{"1:2", "5:6:7", "1:2", "5:6:7"}
	total := int64(0)
	wg := sync.WaitGroup{}
	wg.Add(1)
	cg := inflight.NewCallGroup(func(results map[inflight.ID]*inflight.Response) {
		for _, res := range results {
			subtotal := res.Result.(int)
			atomic.AddInt64(&(total), int64(subtotal))
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
	totalVal := atomic.LoadInt64(&(total))
	if totalVal != 42 {
		// total == (1 + 2) + (5 + 6 + 7) + (1 + 2) + (5 + 6 + 7) == 42
		fmt.Printf("total not equal 42, got:%v \n", totalVal)
	}
	//total == (1 + 2) + (5 + 6 + 7) + (1 + 2) + (5 + 6 + 7) == 42
	fmt.Printf("got the expected amount of %v\n", total)
}
