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

			window := NewOpWindow(3, 3, winTime)

			defer window.Close()
			st := time.Now()
			{
				err := window.Enqueue(ctx, op1_1.Key, op1_1)
				assert.Equal(t, nil, err)
				err = window.Enqueue(ctx, op2_1.Key, op2_1)
				assert.Equal(t, nil, err)
				err = window.Enqueue(ctx, op1_2.Key, op1_2)
				assert.Equal(t, nil, err)
				err = window.Enqueue(ctx, op2_2.Key, op2_2)
				assert.Equal(t, nil, err)
			}

			require.Equal(t, 2, window.Len()) // only 2 unique keys

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

	cg1 := NewCallGroup(func(finalState map[ID]*Response) {})
	cg2 := NewCallGroup(func(finalState map[ID]*Response) {})

	now := time.Now()

	op1_1 := cg1.Add(1, &tsMsg{123, now})
	op1_2 := cg1.Add(2, &tsMsg{111, now})
	op2_1 := cg2.Add(1, &tsMsg{123, now})
	op2_2 := cg2.Add(2, &tsMsg{111, now})

	window := NewOpWindow(3, 3, winTime)

	err := window.Enqueue(ctx, op1_1.Key, op1_1)
	assert.Equal(t, nil, err)
	err = window.Enqueue(ctx, op2_1.Key, op2_1)
	assert.Equal(t, nil, err)
	err = window.Enqueue(ctx, op1_2.Key, op1_2)
	assert.Equal(t, nil, err)
	err = window.Enqueue(ctx, op2_2.Key, op2_2)
	assert.Equal(t, nil, err)

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
}
