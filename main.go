package main

import (
	"context"
	"time"

	"github.com/gocql/gocql"
	"github.com/justnoise/scylla-scanner/scanner"
)

var (
	clusterTimeout  = 20000
	clusterPageSize = 5000
	numWorkers      = 10
	hosts           = []string{}
	username        = ""
	password        = ""
)

// include flags

// -- hosts
// -- username
// -- password
// keyspace
// -- workers
// -- table
// -- extraColumns

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

func partitionCounter(ctx context.Context, row map[string]interface{}) (int, error) {
	return 1, nil
}

func main() {
	keyspace := "system"
	table := "compaction_history"
	partitionKey := "id"
	session, err := getScyllaClient(hosts, username, password)
	if err != nil {
		panic(err)
	}
	scanner := scanner.NewScanner(session, partitionCounter, keyspace, table, partitionKey, []string{})
	scanner.Scan(numWorkers)
}
