package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/justnoise/parallel"
)

var (
	clusterTimeout  = 20000
	clusterPageSize = 5000
	numWorkers      = 10
)

// include flags

// -- hosts
// -- username
// -- password
// keyspace
// -- workers
// -- table

func getScyllaClient(hosts []string, username, password string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.One
	cluster.Timeout = time.Duration(clusterTimeout * 1000 * 1000)
	cluster.PageSize = clusterPageSize
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	// Todo, test out if multiple sessions are faster
	return cluster.CreateSession()
}

func main() {
	producer := &TokenRangeProducer{
		numTokenRanges: uint64(numWorkers * 300),
	}
	workers := make([]parallel.Executor, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = &PartitionCounter{}
	}
	resultHandler := &PartitionCounterResultSummer{}
	workQueue := parallel.NewChanWorkQueue(numWorkers)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := parallel.NewParallelRunner(producer, workers, resultHandler, workQueue)
	err := runner.Run(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Rows: %d, Errors: %d", resultHandler.rows, resultHandler.errors)
}
