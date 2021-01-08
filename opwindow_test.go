package inflight

import (
	"context"
	"fmt"
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

	for _, winTimeT := range winTimes {
		winTime := winTimeT // scope it locally so it can be correctly captured
		t.Run(fmt.Sprintf("windowed_by_%v", winTime), func(t *testing.T) {
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

			window := NewOpWindow(context.Background(), 3, 3, winTime)

			defer window.Close()

			{
				err := window.Enqueue(op1_1.Key, op1_1)
				assert.Equal(t, nil, err)
				err = window.Enqueue(op2_1.Key, op2_1)
				assert.Equal(t, nil, err)
				err = window.Enqueue(op1_2.Key, op1_2)
				assert.Equal(t, nil, err)
				err = window.Enqueue(op2_2.Key, op2_2)
				assert.Equal(t, nil, err)
			}

			st := time.Now()
			require.Equal(t, 2, window.Len()) // only 2 unique keys

			_, ok := window.Dequeue()
			assert.True(t, ok)
			_, ok = window.Dequeue()
			assert.True(t, ok)

			rt := time.Now().Sub(st)
			assert.Greater(t, rt, winTime)
		})
	}
}
