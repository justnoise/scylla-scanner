package scanner

import (
	"context"
	"sync"
)

type PartitionWorkerResultSummer struct {
	mu       sync.Mutex
	modified uint64
	errors   map[string]int
}

func NewPartitionWorkerResultSummer() *PartitionWorkerResultSummer {
	return &PartitionWorkerResultSummer{
		errors: make(map[string]int),
	}
}

func (r *PartitionWorkerResultSummer) Handle(ctx context.Context, iResult interface{}, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := iResult.(PartitionWorkerResult)
	r.modified += result.modified
	for msg, count := range result.errors {
		r.errors[msg] += count
	}
}
