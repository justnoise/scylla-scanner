package scanner

import (
	"context"
	"fmt"
	"strings"

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

func (w *PartitionWorker) getQuery() string {
	extraColumnsArg := ""
	if len(w.extraColumns) > 0 {
		extraColumnsArg = ", " + strings.Join(w.extraColumns, ", ")
	}
	return fmt.Sprintf(queryTemplate, w.partitionKey, w.partitionKey, extraColumnsArg, w.keyspace, w.table, w.partitionKey, w.partitionKey)
}

func (w *PartitionWorker) Do(ctx context.Context, item interface{}) (interface{}, error) {
	tokenRange := item.(tokenRange)
	fmt.Printf("Processing token range %d to %d\n", tokenRange.start, tokenRange.end)
	iter := w.session.Query(w.getQuery(), tokenRange.start, tokenRange.end).Iter()
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
