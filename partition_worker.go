package main

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
)

// Calls callback with the results of the scan on the given partition
type PartitionWorker struct {
	callback     func(context.Context, map[string]interface{}) (int, error)
	keyspace     string
	table        string
	partitionKey string
	session      *gocql.Session
}

type PartitionWorkerResult struct {
	Modified uint64
	Errors   map[string]int
}

func (w *PartitionWorker) Do(ctx context.Context, item interface{}) (interface{}, error) {
	tokenRange := item.(tokenRange)
	fmt.Printf("Processing token range %d to %d\n", tokenRange.start, tokenRange.end)
	query := fmt.Sprintf(queryTemplate, w.partitionKey, w.partitionKey, w.keyspace, w.table, w.partitionKey, w.partitionKey)
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
		Modified: modifiedCounter,
		Errors:   errorCounter,
	}, nil
}
