package inflight

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpWindow(t *testing.T) {
	t.Parallel()

	winTimes := []time.Duration{
		time.Duration(0),
		1 * time.Millisecond,
		10 * time.Millisecond,
		100 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
	}

	for _, winTime := range winTimes {
		winTime := winTime // scope it locally so it can be correctly captured
		t.Run(fmt.Sprintf("windowed_by_%v", winTime), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			completed1 := 0
			completed2 := 0
			cg1 := NewCallGroup(func(map[ID]*Response) {
				completed1++
			})

			cg2 := NewCallGroup(func(map[ID]*Response) {
				completed2++
			})

			now := time.Now()

			op1_1 := cg1.Add(1, &tsMsg{123, now})
			op1_2 := cg1.Add(2, &tsMsg{111, now})
			op2_1 := cg2.Add(1, &tsMsg{123, now})
			op2_2 := cg2.Add(2, &tsMsg{111, now})

			window := NewOpWindow(3, 3, winTime)
			t.Cleanup(window.Close)

			st := time.Now()
			{
				err := window.Enqueue(ctx, op1_1.Key, op1_1)
				require.NoError(t, err)
				err = window.Enqueue(ctx, op2_1.Key, op2_1)
				require.NoError(t, err)
				err = window.Enqueue(ctx, op1_2.Key, op1_2)
				require.NoError(t, err)
				err = window.Enqueue(ctx, op2_2.Key, op2_2)
				require.NoError(t, err)
			}

			require.Equal(t, 2, window.q.Len()) // only 2 unique keys

			_, err := window.Dequeue(ctx)
			assert.NoError(t, err)
			_, err = window.Dequeue(ctx)
			assert.NoError(t, err)

			rt := time.Since(st)
			assert.Greater(t, rt, winTime)
		})
	}
}

func TestOpWindowClose(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	winTime := 100 * time.Hour // we want everything to hang until we close the queue.

	cg1 := NewCallGroup(func(map[ID]*Response) {})
	cg2 := NewCallGroup(func(map[ID]*Response) {})

	now := time.Now()

	op1_1 := cg1.Add(1, &tsMsg{123, now})
	op1_2 := cg1.Add(2, &tsMsg{111, now})
	op2_1 := cg2.Add(1, &tsMsg{123, now})
	op2_2 := cg2.Add(2, &tsMsg{111, now})

	window := NewOpWindow(3, 3, winTime)

	err := window.Enqueue(ctx, op1_1.Key, op1_1)
	require.NoError(t, err)
	err = window.Enqueue(ctx, op2_1.Key, op2_1)
	require.NoError(t, err)
	err = window.Enqueue(ctx, op1_2.Key, op1_2)
	require.NoError(t, err)
	err = window.Enqueue(ctx, op2_2.Key, op2_2)
	require.NoError(t, err)

	var ops uint64
	var closes uint64
	const workers int = 12
	for i := 0; i < workers; i++ {
		go func() {
			for {
				if _, err := window.Dequeue(ctx); err != nil {
					require.ErrorIs(t, err, ErrQueueClosed)
					atomic.AddUint64(&closes, 1)
					return
				}
				atomic.AddUint64(&ops, 1)
			}
		}()
	}

	time.Sleep(1000 * time.Millisecond)
	assert.Equal(t, uint64(0), atomic.LoadUint64(&ops)) // nothing should have been dequeued yet

	window.Close()
	time.Sleep(1000 * time.Millisecond)
	assert.Equal(t, uint64(workers), atomic.LoadUint64(&closes))
	assert.Equal(t, uint64(2), atomic.LoadUint64(&ops)) // 2 uniq keys are enqueued

	err = window.Enqueue(ctx, op1_1.Key, op1_1)
	require.ErrorIs(t, err, ErrQueueClosed)
}

func TestOpWindowErrQueueSaturatedWidth(t *testing.T) {
	t.Parallel()
	cg := NewCallGroup(func(map[ID]*Response) {})
	now := time.Now()

	op1 := cg.Add(1, &tsMsg{123, now})
	op2 := cg.Add(1, &tsMsg{123, now})

	window := NewOpWindow(2, 1, time.Millisecond)
	ctx := context.Background()
	err := window.Enqueue(ctx, op1.Key, op1)
	require.NoError(t, err)

	err = window.Enqueue(ctx, op2.Key, op2)
	require.ErrorIs(t, err, ErrQueueSaturatedWidth)

	_, err = window.Dequeue(ctx)
	require.NoError(t, err)

	err = window.Enqueue(ctx, op2.Key, op2)
	require.NoError(t, err)
}

func TestOpWindowErrQueueSaturatedDepth(t *testing.T) {
	t.Parallel()
	cg := NewCallGroup(func(map[ID]*Response) {})
	now := time.Now()
	op1 := cg.Add(1, &tsMsg{123, now})
	op2 := cg.Add(2, &tsMsg{234, now})

	window := NewOpWindow(1, 1, time.Millisecond)
	ctx := context.Background()
	err := window.Enqueue(ctx, op1.Key, op1)
	require.NoError(t, err)

	ctx1, cancel := context.WithTimeout(ctx, time.Millisecond) // let it run for a sec for coverage ¯\_(ツ)_/¯
	defer cancel()
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-ctx1.Done()
		window.queueHasSpace <- struct{}{} // emulate queue having space then filling back up
		cancel()
	}()

	err = window.Enqueue(ctx, op2.Key, op2)
	require.ErrorIs(t, err, ErrQueueSaturatedDepth)

	_, err = window.Dequeue(ctx)
	require.NoError(t, err)

	err = window.Enqueue(ctx, op2.Key, op2)
	require.NoError(t, err)
}

func TestOpWindowErrQueueSaturatedDepthClose(t *testing.T) {
	t.Parallel()
	cg := NewCallGroup(func(map[ID]*Response) {})
	now := time.Now()
	op1 := cg.Add(1, &tsMsg{123, now})
	op2 := cg.Add(2, &tsMsg{234, now})

	window := NewOpWindow(1, 1, time.Millisecond)
	ctx := context.Background()
	err := window.Enqueue(ctx, op1.Key, op1)
	require.NoError(t, err)

	go func() {
		time.Sleep(time.Millisecond)
		window.Close()
	}()

	err = window.Enqueue(ctx, op2.Key, op2)
	require.ErrorIs(t, err, ErrQueueClosed)
}

func TestOpWindowDequeueEmptyQueue(t *testing.T) {
	t.Parallel()
	window := NewOpWindow(1, 1, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := window.Dequeue(ctx)
	require.ErrorIs(t, err, ctx.Err())
}
