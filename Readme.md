# Inflight - primitives for coordinating interdependent operations in distributed systems

The package inflight provides primitives(data strutures) for managing inflight operations that
are being processed in a distrubuted system.

[![GoDoc](https://godoc.org/github.com/lytics/inflight?status.svg)](http://godoc.org/github.com/lytics/inflight)
[![Build Status](https://travis-ci.org/lytics/inflight.svg?branch=master)](https://travis-ci.org/lytics/inflight)
[![Code Coverage](https://codecov.io/gh/lytics/inflight/branch/master/graph/badge.svg)](https://codecov.io/gh/lytics/inflight)
[![Go ReportCard](https://goreportcard.com/badge/lytics/inflight)](https://goreportcard.com/report/lytics/inflight)

## CallGroup

CallGroup spawns off a group of operations for each call to `Add()` and
calls the `CallGroupCompletion` func when the last operation have
completed.  The CallGroupCompletion func can be thought of as a finalizer where
one can gather errors and/or results from the function calls.

Helpful when you want to map out operations to multiple go routines, and then have the set of operations reduced when all have completed.

 Example Usage:
```go
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
```


## Opqueue

OpQueue is a thread-safe duplicate operation suppression queue, that combines
duplicate operations (queue entires) into sets that will be dequeued together.

For example, If you enqueue an item with a key that already exists, then that
item will be appended to that key's set of items. Otherwise the item is
inserted into the head of the list as a new item.

On Dequeue a SET is returned of all items that share a key in the queue.
It blocks on dequeue if the queue is empty, but returns an error if the
queue is full during enqueue.
 
```
       +------------Width------------>
       + +-----+
       | |ID   |
       | |923  |
       | +-----+
       |    |
       |    |
       |    v
Depth  | +-----+    +-----+    +-----+
       | |ID   +---->ID   +---->ID   |
       | |424  |    |424  |    |424  |
       | +-----+    +-----+    +-----+
       |    |
       |    |
       |    v
       | +-----+
       | |ID   |
       | |99   |
       | +-----+
       v
```





