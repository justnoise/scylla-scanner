package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/gocql/gocql"
)

type PartitionCounter struct {
	keyspace     string
	table        string
	partitionKey string
	session      *gocql.Session
}

func (w *PartitionCounter) Do(ctx context.Context, item interface{}) (interface{}, error) {
	tokenRange := item.(tokenRange)
	fmt.Printf("Processing token range %d to %d\n", tokenRange.start, tokenRange.end)
	query := fmt.Sprintf(queryTemplate, w.partitionKey, w.partitionKey, w.keyspace, w.table, w.partitionKey, w.partitionKey)
	scanner := w.session.Query(query, tokenRange.start, tokenRange.end).Iter().Scanner()
	var rowCount, errorCount uint64
	var token int64
	for scanner.Next() {
		err := scanner.Scan(&token)
		if err != nil {
			errorCount++
		} else {
			rowCount++
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return PartitionCounterResult{}, nil
}

type PartitionCounterResult struct {
	rows   uint64
	errors uint64
}

type PartitionCounterResultSummer struct {
	mu     sync.Mutex
	rows   uint64
	errors uint64
}

func (r *PartitionCounterResultSummer) Handle(ctx context.Context, iResult interface{}, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := iResult.(PartitionCounterResult)
	r.rows += result.rows
	r.errors += result.errors
}
