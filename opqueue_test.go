package inflight

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bmizerany/assert"
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

	op1_1 := cg1.Add(1, &tsMsg{123, 5, "user", 1234567})
	op1_2 := cg1.Add(2, &tsMsg{111, 6, "user", 2222222})
	op2_1 := cg2.Add(1, &tsMsg{123, 5, "user", 1234567})
	op2_2 := cg2.Add(2, &tsMsg{111, 6, "user", 2222222})

	opq := NewOpQueue(10, 10)
	defer opq.Close()

	{
		err := opq.Enqueue(op1_1.Key, op1_1)
		assert.T(t, err == nil)
		err = opq.Enqueue(op2_1.Key, op2_1)
		assert.T(t, err == nil)
		err = opq.Enqueue(op1_2.Key, op1_2)
		assert.T(t, err == nil)
		err = opq.Enqueue(op2_2.Key, op2_2)
		assert.T(t, err == nil)
	}

	set1, open := opq.Dequeue()
	assert.T(t, open)
	set2, open := opq.Dequeue()
	assert.T(t, open)

	{
		assert.T(t, len(set1.Ops()) == 2)
		assert.T(t, len(set2.Ops()) == 2)
	}

	{
		//The sets should be made of one item of each callgroup, so we'll
		//have to complete both sets before we expect complete[1,2] to increment
		assert.T(t, completed1 == 0)
		assert.T(t, completed2 == 0)
		set := set1.Ops()
		set[0].Finish(nil, nil)
		set[1].Finish(nil, nil)
		assert.T(t, completed1 == 0)
		assert.T(t, completed2 == 0)

		set = set2.Ops()
		set[0].Finish(nil, nil)
		set[1].Finish(nil, nil)
		assert.T(t, completed1 == 1)
		assert.T(t, completed2 == 1)
	}
}

func TestOpQueueClose(t *testing.T) {
	t.Parallel()
	completed1 := 0
	cg1 := NewCallGroup(func(finalState map[ID]*Response) {
		completed1++
	})

	opq := NewOpQueue(10, 10)

	for i := 0; i < 9; i++ {
		op := cg1.Add(uint64(i), &tsMsg{i, i, "user", 2222222})
		err := opq.Enqueue(op.Key, op)
		assert.T(t, err == nil)
	}

	timer := time.AfterFunc(5*time.Second, func() {
		t.Fatalf("testcase timed out after 5 secs.")
	})
	for i := 0; i < 9; i++ {
		set1, open := opq.Dequeue()
		assert.T(t, open)
		assert.Tf(t, len(set1.Ops()) == 1, " at loop:%v set1_len:%v", i, len(set1.Ops()))
	}
	timer.Stop()

	st := time.Now()
	time.AfterFunc(10*time.Millisecond, func() {
		opq.Close() // calling close should release the call to opq.Dequeue()
	})
	set1, open := opq.Dequeue() //this call should hang until we call Close above.
	assert.T(t, open == false, "expect:false got:%v", open)
	assert.T(t, set1 == nil)
	rt := time.Since(st)
	assert.Tf(t, rt >= 10*time.Millisecond, "we shouldn't have returned until Close was called: returned after:%v", rt)

}

func TestOpQueueFull(t *testing.T) {
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
	for i := 0; i < 100; i++ {
		op := cg1.Add(uint64(i), &tsMsg{i, i, "user", 2222222})
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
		assert.T(t, open)
		assert.Tf(t, len(set1.Ops()) == 1, " at loop:%v set1_len:%v", i, len(set1.Ops()))
	}
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

	startingLine := sync.WaitGroup{}
	startingLine.Add(1) // block all go routines until the loop has finished spinning them up.

	finishLine, finish := context.WithCancel(context.Background())
	dequeFinishLine, deqFinish := context.WithCancel(context.Background())
	const concurrency = 2
	for w := 0; w < concurrency; w++ {
		go func() {
			startingLine.Wait()
			for i := 0; i < 1000000; i++ {
				select {
				case <-finishLine.Done():
					return
				default:
				}
				op := cg1.Add(uint64(i), &tsMsg{i, i, "user", 2222222})
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
		}()
	}

	for w := 0; w < concurrency; w++ {
		go func() {
			startingLine.Wait()
			for {
				select {
				case <-dequeFinishLine.Done():
					return
				default:
				}
				set1, open := opq.Dequeue()
				assert.T(t, open)
				dequeueCnt.IncrBy(len(set1.Ops()))
				if len(set1.Ops()) > 1 {
					mergeCnt.Incr()
				}
			}
		}()
	}
	startingLine.Done() //release all the waiting workers.

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
