package inflight

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lytics/inflight/testutils"
)

/*
To insure consistency I suggest running the test for a while with the following,
and if after 5 mins it never fails then we know the testcases are consistent.
  while go test -v  --race ; do echo `date` ; done
*/

func TestOpQueue(t *testing.T) {
	t.Parallel()
	completed1 := 0
	completed2 := 0
	cg1 := NewCallGroup(func(finalState map[ID]*Response) {
		completed1++
	})

	cg2 := NewCallGroup(func(finalState map[ID]*Response) {
		completed2++
	})

	now := time.Now()
	op1_1 := cg1.Add(1, &tsMsg{123, now})
	op1_2 := cg1.Add(2, &tsMsg{111, now})
	op2_1 := cg2.Add(1, &tsMsg{123, now})
	op2_2 := cg2.Add(2, &tsMsg{111, now})

	opq := NewOpQueue(10, 10)
	defer opq.Close()

	{
		err := opq.Enqueue(op1_1.Key, op1_1)
		assert.Equal(t, nil, err)
		err = opq.Enqueue(op2_1.Key, op2_1)
		assert.Equal(t, nil, err)
		err = opq.Enqueue(op1_2.Key, op1_2)
		assert.Equal(t, nil, err)
		err = opq.Enqueue(op2_2.Key, op2_2)
		assert.Equal(t, nil, err)
		assert.Equal(t, 2, opq.Len()) // only 2 keys
	}

	set1, open := opq.Dequeue()
	assert.True(t, open)
	set2, open := opq.Dequeue()
	assert.True(t, open)

	{
		assert.Equal(t, 2, len(set1.Ops()))
		assert.Equal(t, 2, len(set2.Ops()))
	}

	{
		//The sets should be made of one item of each callgroup, so we'll
		//have to complete both sets before we expect complete[1,2] to increment
		assert.Equal(t, 0, completed1)
		assert.Equal(t, 0, completed2)
		set := set1.Ops()
		set[0].Finish(nil, nil)
		set[1].Finish(nil, nil)
		assert.Equal(t, 0, completed1)
		assert.Equal(t, 0, completed2)

		set2.FinishAll(nil, nil)
		assert.Equal(t, 1, completed1)
		assert.Equal(t, 1, completed2)
	}

}

func TestOpQueueClose(t *testing.T) {
	t.Parallel()
	completed1 := 0
	cg1 := NewCallGroup(func(finalState map[ID]*Response) {
		completed1++
	})

	opq := NewOpQueue(10, 10)
	now := time.Now()

	for i := 0; i < 9; i++ {
		op := cg1.Add(uint64(i), &tsMsg{uint64(i), now})
		err := opq.Enqueue(op.Key, op)
		assert.Equal(t, nil, err)
	}

	timer := time.AfterFunc(5*time.Second, func() {
		t.Fatalf("testcase timed out after 5 secs.")
	})
	for i := 0; i < 9; i++ {
		set1, open := opq.Dequeue()
		assert.True(t, open)
		assert.Equal(t, 1, len(set1.Ops()), " at loop:%v set1_len:%v", i, len(set1.Ops()))
	}
	timer.Stop()

	st := time.Now()
	time.AfterFunc(10*time.Millisecond, func() {
		opq.Close() // calling close should release the call to opq.Dequeue()
	})
	set1, open := opq.Dequeue() //this call should hang until we call Close above.
	assert.Equal(t, false, open)
	assert.True(t, nil == set1)
	rt := time.Since(st)
	assert.True(t, rt >= 10*time.Millisecond, "we shouldn't have returned until Close was called: returned after:%v", rt)

}

func TestOpQueueFullDepth(t *testing.T) {
	t.Parallel()
	completed1 := 0
	cg1 := NewCallGroup(func(finalState map[ID]*Response) {
		completed1++
	})

	opq := NewOpQueue(10, 10)
	defer opq.Close()

	succuess := 0
	depthErrors := 0
	widthErrors := 0
	now := time.Now()

	for i := 0; i < 100; i++ {
		op := cg1.Add(uint64(i), &tsMsg{uint64(i), now})
		err := opq.Enqueue(op.Key, op)
		switch err {
		case nil:
			succuess++
		case ErrQueueSaturatedDepth:
			depthErrors++
		case ErrQueueSaturatedWidth:
			widthErrors++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	assert.Equalf(t, succuess, 10, "expected 10, got:%v", succuess)
	assert.Equalf(t, depthErrors, 90, "expected 90, got:%v", depthErrors)
	assert.Equalf(t, widthErrors, 0, "expected 0, got:%v", widthErrors)

	timer := time.AfterFunc(5*time.Second, func() {
		t.Fatalf("testcase timed out after 5 secs.")
	})
	for i := 0; i < succuess; i++ {
		set1, open := opq.Dequeue()
		assert.True(t, open)
		assert.Equal(t, 1, len(set1.Ops()), " at loop:%v set1_len:%v", i, len(set1.Ops()))
	}
	timer.Stop()
}

// TestOpQueueFullWidth exactly like the test above, except we enqueue the SAME ID each time,
// so that we get ErrQueueSaturatedWidth errrors instead of ErrQueueSaturatedDepth errors.
func TestOpQueueFullWidth(t *testing.T) {
	t.Parallel()
	completed1 := 0
	cg1 := NewCallGroup(func(finalState map[ID]*Response) {
		completed1++
	})

	opq := NewOpQueue(10, 10)
	defer opq.Close()

	succuess := 0
	depthErrors := 0
	widthErrors := 0
	now := time.Now()

	for i := 0; i < 100; i++ {
		op := cg1.Add(1, &tsMsg{uint64(i), now})
		err := opq.Enqueue(op.Key, op)
		switch err {
		case nil:
			succuess++
		case ErrQueueSaturatedDepth:
			depthErrors++
		case ErrQueueSaturatedWidth:
			widthErrors++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	assert.Equalf(t, succuess, 10, "expected 10, got:%v", succuess)
	assert.Equalf(t, depthErrors, 0, "expected 90, got:%v", depthErrors)
	assert.Equalf(t, widthErrors, 90, "expected 0, got:%v", widthErrors)

	timer := time.AfterFunc(5*time.Second, func() {
		t.Fatalf("testcase timed out after 5 secs.")
	})

	found := 0
	set1, open := opq.Dequeue()
	assert.True(t, open)
	assert.Equal(t, 10, len(set1.Ops()), " at loop:%v set1_len:%v", succuess, len(set1.Ops())) // max width is 10, so we should get 10 in the first batch
	found += len(set1.Ops())

	timer.Stop()
}

func TestOpQueueForRaceDetection(t *testing.T) {
	t.Parallel()
	completed1 := 0
	cg1 := NewCallGroup(func(finalState map[ID]*Response) {
		completed1++
	})

	enqueueCnt := testutils.AtomicInt{}
	dequeueCnt := testutils.AtomicInt{}
	mergeCnt := testutils.AtomicInt{}
	depthErrorCnt := testutils.AtomicInt{}
	widthErrorCnt := testutils.AtomicInt{}

	opq := NewOpQueue(300, 500)
	defer opq.Close()

	startingLine1 := sync.WaitGroup{}
	startingLine2 := sync.WaitGroup{}
	// block all go routines until the loop has finished spinning them up.
	startingLine1.Add(1)
	startingLine2.Add(1)

	finishLine, finish := context.WithCancel(context.Background())
	dequeFinishLine, deqFinish := context.WithCancel(context.Background())
	const concurrency = 2
	now := time.Now()

	for w := 0; w < concurrency; w++ {
		go func(w int) {
			startingLine1.Wait()
			for i := 0; i < 1000000; i++ {
				select {
				case <-finishLine.Done():
					t.Logf("worker %v exiting at %v", w, i)
					return
				default:
				}
				op := cg1.Add(uint64(i), &tsMsg{uint64(i), now})
				err := opq.Enqueue(op.Key, op)
				switch err {
				case nil:
					enqueueCnt.Incr()
				case ErrQueueSaturatedDepth:
					depthErrorCnt.Incr()
				case ErrQueueSaturatedWidth:
					widthErrorCnt.Incr()
				default:
					t.Fatalf("unexpected error: %v", err)
				}
			}
		}(w)
	}

	for w := 0; w < concurrency; w++ {
		go func() {
			startingLine2.Wait()
			for {
				select {
				case <-dequeFinishLine.Done():
					return
				default:
				}
				set1, open := opq.Dequeue()
				select {
				case <-dequeFinishLine.Done():
					return
				default:
				}
				assert.True(t, open)
				dequeueCnt.IncrBy(len(set1.Ops()))
				if len(set1.Ops()) > 1 {
					mergeCnt.Incr()
				}
			}
		}()
	}
	startingLine1.Done() //release all the waiting workers.
	startingLine2.Done() //release all the waiting workers.

	const runtime = 2
	timeout := time.AfterFunc((runtime+10)*time.Second, func() {
		t.Fatalf("testcase timed out after 5 secs.")
	})
	defer timeout.Stop()

	//let the testcase run for N seconds
	time.AfterFunc(runtime*time.Second, func() {
		finish()
	})
	<-finishLine.Done()
	// Sleep to give the dequeue workers plenty of time to drain the queue before exiting.
	time.Sleep(500 * time.Millisecond)
	deqFinish()

	enq := enqueueCnt.Get()
	deq := dequeueCnt.Get()
	if enq != deq {
		t.Fatalf("enqueueCnt and dequeueCnt should match: enq:% deq:%v", enq, deq)
	}
	// NOTE: I get the following performance on my laptop:
	//       opqueue_test.go:275: enqueue errors: 137075 mergedMsgs:2553 enqueueCnt:231437 dequeueCnt:231437 rate:115718 msgs/sec
	//       Over 100k msg a sec is more than fast enough for linkgrid...
	t.Logf("enqueue errors: [depth:%v width:%v] mergedMsgs:%v enqueueCnt:%v dequeueCnt:%v rate:%v msgs/sec", depthErrorCnt.Get(), widthErrorCnt.Get(), mergeCnt.Get(), enq, deq, enq/runtime)
}

func TestOpWindowCloseConcurrent(t *testing.T) {
	t.Parallel()

	cg1 := NewCallGroup(func(finalState map[ID]*Response) {})
	cg2 := NewCallGroup(func(finalState map[ID]*Response) {})

	now := time.Now()

	op1_1 := cg1.Add(1, &tsMsg{123, now})
	op1_2 := cg1.Add(2, &tsMsg{111, now})
	op2_1 := cg2.Add(1, &tsMsg{123, now})
	op2_2 := cg2.Add(2, &tsMsg{111, now})

	oq := NewOpQueue(300, 500)

	var ops uint64
	var closes uint64
	const workers int = 12
	for i := 0; i < workers; i++ {
		go func() {
			for {
				e, ok := oq.Dequeue()
				if e != nil {
					assert.True(t, ok)
					atomic.AddUint64(&ops, 1)
				} else {
					assert.False(t, ok)
					break
				}
			}
			atomic.AddUint64(&closes, 1)
		}()
	}

	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, uint64(0), atomic.LoadUint64(&ops)) // nothing should have been dequeued yet
	assert.Equal(t, uint64(0), atomic.LoadUint64(&closes))

	err := oq.Enqueue(op1_1.Key, op1_1)
	assert.Equal(t, nil, err)
	err = oq.Enqueue(op2_1.Key, op2_1)
	assert.Equal(t, nil, err)
	err = oq.Enqueue(op1_2.Key, op1_2)
	assert.Equal(t, nil, err)
	err = oq.Enqueue(op2_2.Key, op2_2)
	assert.Equal(t, nil, err)

	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, uint64(2), atomic.LoadUint64(&ops)) // 2 uniq keys are enqueued
	assert.Equal(t, uint64(0), atomic.LoadUint64(&closes))

	oq.Close()
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, uint64(2), atomic.LoadUint64(&ops)) // we still only had 2 uniq keys seen
	assert.Equal(t, uint64(workers), atomic.LoadUint64(&closes))
}
