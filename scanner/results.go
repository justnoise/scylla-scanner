package scanner

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

type ComposableResult interface {
	Combine(ComposableResult)
	Add(interface{}, error)
	Handle(context.Context, interface{}, error)
	String() string
}

type ResultFactory func() ComposableResult

type IntResult struct {
	mu       sync.Mutex
	modified uint64
	errors   map[string]int
}

var _ ComposableResult = &IntResult{}
var _ ResultFactory = NewIntResult

func (r *IntResult) Add(iResult interface{}, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.modified += iResult.(uint64)
	if err != nil {
		r.errors[err.Error()]++
	}
}

func (r *IntResult) Combine(other ComposableResult) {
	otherResult := other.(*IntResult)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.modified += otherResult.modified
	for msg, count := range otherResult.errors {
		r.errors[msg] += count
	}
}

func NewIntResult() ComposableResult {
	return &IntResult{
		errors: make(map[string]int),
	}
}

func (r *IntResult) Handle(ctx context.Context, iResult interface{}, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := iResult.(*IntResult)
	r.modified += result.modified
	for msg, count := range result.errors {
		r.errors[msg] += count
	}
}

func (r *IntResult) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "Modified: %d", r.modified)
	for msg, ct := range r.errors {
		fmt.Fprintf(&b, "Error: %s: %d", msg, ct)
	}
	return b.String()
}
