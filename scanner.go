package main

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/justnoise/parallel"
)

type Scanner struct {
	callback     PartitionCallback
	session      *gocql.Session
	keyspace     string
	table        string
	partitionKey string
	extraColumns []string
}

func NewScanner(session *gocql.Session, callback PartitionCallback, keyspace, table, partitionKey string, extraColumns []string) *Scanner {
	return &Scanner{
		callback:     callback,
		session:      session,
		keyspace:     keyspace,
		table:        table,
		partitionKey: partitionKey,
		extraColumns: extraColumns,
	}
}

func (s *Scanner) Scan() {
	producer := &TokenRangeProducer{
		numTokenRanges: uint64(numWorkers * 300),
	}
	workers := make([]parallel.Executor, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = NewPartitionWorker(s.callback, s.session, s.keyspace, s.table, s.partitionKey, s.extraColumns)
	}
	resultHandler := NewPartitionWorkerResultSummer()
	workQueue := parallel.NewChanWorkQueue(numWorkers)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := parallel.NewParallelRunner(producer, workers, resultHandler, workQueue)
	err := runner.Run(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Modified: %d", resultHandler.modified)
	fmt.Printf("Errors: %d", resultHandler.errors)
}
