package inflight

import (
	"context"
	"testing"
	"time"

	"github.com/bmizerany/assert"
)

func TestWgEmpty(t *testing.T) {
	wg := NewWg()
	e := wg.Wait(context.Background())
	assert.Tf(t, e == nil, "")

	//time.AfterFunc()
}

func TestWgOne(t *testing.T) {
	wg := NewWg()
	e := wg.Inc()
	assert.Tf(t, e == nil, "")

	st := time.Now()
	time.AfterFunc(200*time.Millisecond, func() { wg.Dec() })
	e = wg.Wait(context.Background())
	assert.Tf(t, e == nil, "")
	assert.Tf(t, time.Since(st) >= (200*time.Millisecond), "")
}

func TestWgCtxTimeout(t *testing.T) {
	wg := NewWg()
	e := wg.Inc()
	assert.Tf(t, e == nil, "")

	st := time.Now()

	ctx, can := context.WithTimeout(context.Background(), 200*time.Millisecond)
	time.AfterFunc(800*time.Millisecond, func() { t.Fatalf("testcase timed out") })

	defer can()
	e = wg.Wait(ctx)
	assert.Tf(t, e == context.DeadlineExceeded, "err: %v", e)
	assert.Tf(t, time.Since(st) >= (200*time.Millisecond), "got: %v", time.Since(st))
}
