package scanner

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/justnoise/parallel"
)

var (
	WorkItemsPerWorker = 300
)

type Scanner struct {
	callback      PartitionCallback
	session       *gocql.Session
	queryBuilder  QueryBuilder
	resultFactory ResultFactory
}

func NewBasicScanner(session *gocql.Session, callback PartitionCallback, keyspace, table, partitionKey string) *Scanner {
	queryBuilder := &BasicQueryBuilder{
		Keyspace:     keyspace,
		Table:        table,
		PartitionKey: partitionKey,
	}
	return NewScanner(session, callback, queryBuilder, NewIntResult)
}

func NewScanner(session *gocql.Session, callback PartitionCallback, queryBuilder QueryBuilder, resultFactory ResultFactory) *Scanner {
	return &Scanner{
		callback:      callback,
		session:       session,
		queryBuilder:  queryBuilder,
		resultFactory: resultFactory,
	}
}

func (s *Scanner) Scan(numWorkers int) {
	producer := &TokenRangeProducer{
		numTokenRanges: uint64(numWorkers * WorkItemsPerWorker),
	}
	workers := make([]parallel.Executor, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = NewPartitionWorker(s.callback, s.session, s.queryBuilder, s.resultFactory)
	}
	resultHandler := s.resultFactory()
	workQueue := parallel.NewChanWorkQueue(numWorkers)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := parallel.NewParallelRunner(producer, workers, resultHandler, workQueue)
	err := runner.Run(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(resultHandler.String())
}
