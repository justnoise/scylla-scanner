package scanner

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
)

type PartitionCallback func(context.Context, map[string]interface{}) (interface{}, error)

// The PartitionWorker calls the callback with each row of the scan. Results are aggregated
// into the results object that's created by ResultFactory.
type PartitionWorker struct {
	callback      PartitionCallback
	session       *gocql.Session
	queryBuilder  QueryBuilder
	resultFactory ResultFactory
}

func NewPartitionWorker(callback PartitionCallback, session *gocql.Session, queryBuilder QueryBuilder, resultFactory ResultFactory) *PartitionWorker {
	return &PartitionWorker{
		callback:      callback,
		session:       session,
		queryBuilder:  queryBuilder,
		resultFactory: resultFactory,
	}
}

func (w *PartitionWorker) Do(ctx context.Context, item interface{}) (interface{}, error) {
	tokenRange := item.(tokenRange)
	fmt.Printf("Processing token range %d to %d\n", tokenRange.start, tokenRange.end)
	iter := w.session.Query(w.queryBuilder.Build(), tokenRange.start, tokenRange.end).Iter()
	result := w.resultFactory()
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}
		modified, err := w.callback(ctx, row)
		result.Add(modified, err)
	}
	return result, nil
}
