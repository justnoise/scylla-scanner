package main

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/gocql/gocql"
)

const (
	queryTemplate = "SELECT DISTINCT token(%s), %s%s FROM %s.%s WHERE token(%s) >= ? AND token(%s) <= ?"
)

type PartitionCallback func(context.Context, map[string]interface{}) (int, error)

// Calls callback with each row of the scan
type PartitionWorker struct {
	callback     PartitionCallback
	session      *gocql.Session
	keyspace     string
	table        string
	partitionKey string
	extraColumns []string
}

func NewPartitionWorker(callback PartitionCallback, session *gocql.Session, keyspace, table, partitionKey string, extraColumns []string) *PartitionWorker {
	return &PartitionWorker{
		callback:     callback,
		session:      session,
		keyspace:     keyspace,
		table:        table,
		partitionKey: partitionKey,
		extraColumns: extraColumns,
	}
}

type PartitionWorkerResult struct {
	modified uint64
	errors   map[string]int
}

func (w *PartitionWorker) Do(ctx context.Context, item interface{}) (interface{}, error) {
	tokenRange := item.(tokenRange)
	fmt.Printf("Processing token range %d to %d\n", tokenRange.start, tokenRange.end)
	extraColumnsArg := ""
	if len(w.extraColumns) > 0 {
		extraColumnsArg = ", " + strings.Join(w.extraColumns, ", ")
	}
	query := fmt.Sprintf(queryTemplate, w.partitionKey, w.partitionKey, extraColumnsArg, w.keyspace, w.table, w.partitionKey, w.partitionKey)
	iter := w.session.Query(query, tokenRange.start, tokenRange.end).Iter()
	var modifiedCounter uint64
	errorCounter := make(map[string]int)
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}
		modified, err := w.callback(ctx, row)
		if err != nil {
			errorCounter[err.Error()]++
		} else {
			modifiedCounter += uint64(modified)
		}
	}
	return PartitionWorkerResult{
		modified: modifiedCounter,
		errors:   errorCounter,
	}, nil
}

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
