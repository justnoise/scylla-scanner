package scanner

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

type WorkerResultHandler struct {
	UserResult ComposableResult
}

func (r *WorkerResultHandler) Handle(ctx context.Context, iResult interface{}, err error) {
	result := iResult.(ComposableResult)
	r.UserResult.Combine(result)
}

type ComposableResult interface {
	// Adds the output of the callback to the result and aggregates errors
	Add(interface{}, error)
	// Combines multiple ComoposableResults
	Combine(ComposableResult)
	// Returns a string representation of the result at the end of the run
	String() string
}

type ResultFactory func() ComposableResult

type IntResult struct {
	mu     sync.Mutex
	count  uint64
	errors map[string]int
}

var _ ComposableResult = &IntResult{}
var _ ResultFactory = NewIntResult

func NewIntResult() ComposableResult {
	return &IntResult{
		errors: make(map[string]int),
	}
}

func (r *IntResult) Add(iResult interface{}, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count += iResult.(uint64)
	if err != nil {
		r.errors[err.Error()]++
	}
}

func (r *IntResult) Combine(other ComposableResult) {
	otherResult := other.(*IntResult)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count += otherResult.count
	for msg, count := range otherResult.errors {
		r.errors[msg] += count
	}
}

func (r *IntResult) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "Count: %d\n", r.count)
	for msg, ct := range r.errors {
		fmt.Fprintf(&b, "Error: %s: %d\n", msg, ct)
	}
	return b.String()
}
