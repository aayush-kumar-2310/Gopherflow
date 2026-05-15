package worker

import (
	"context"
	"sync"

	"Shared/metrics"
)

const poolService = "event-handler"

// Pool limits concurrent stage executions.
type Pool struct {
	sem chan struct{}
	wg  sync.WaitGroup
}

func NewPool(size int) *Pool {
	if size < 1 {
		size = 1
	}
	return &Pool{sem: make(chan struct{}, size)}
}

func (p *Pool) Go(ctx context.Context, fn func(context.Context)) {
	if ctx.Err() != nil {
		return
	}
	select {
	case <-ctx.Done():
		return
	case p.sem <- struct{}{}:
	}

	metrics.WorkerPoolInFlight.WithLabelValues(poolService).Inc()
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() { <-p.sem }()
		defer metrics.WorkerPoolInFlight.WithLabelValues(poolService).Dec()
		fn(ctx)
	}()
}

func (p *Pool) Wait() {
	p.wg.Wait()
}
