package main

import (
	"context"
	"time"

	"github.com/gocql/gocql"
	"github.com/jessevdk/go-flags"
	"github.com/justnoise/scylla-scanner/scanner"
)

var (
	clusterTimeout  = 20000
	clusterPageSize = 5000
	opts            struct {
		NumWorkers   int      `short:"w" long:"num-workers" description:"Number of workers to use" default:"48" env:"NUM_WORKERS"`
		Hosts        []string `short:"h" long:"hosts" description:"Comma separated list of scylla hosts" env:"SCYLLA_HOSTS" env-delim:"," required:"true"`
		Username     string   `short:"u" long:"username" description:"Scylla username" env:"SCYLLA_USERNAME"`
		Password     string   `short:"p" long:"password" description:"Scylla password" env:"SCYLLA_PASSWORD"`
		Keyspace     string   `short:"k" long:"keyspace" description:"keyspace that contains the table to scan" env:"SCYLLA_KEYSPACE" required:"true"`
		Table        string   `short:"t" long:"table" description:"name of the table to scan" env:"SCYLLA_TABLE" required:"true"`
		PartitionKey string   `long:"partition-key" description:"comma separated list of columns in the table's partition key" env:"PARTITION_KEY" required:"true"`
	}
)

func getScyllaClient(hosts []string, username, password string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.One
	cluster.Timeout = time.Duration(clusterTimeout * 1000 * 1000)
	cluster.PageSize = clusterPageSize
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	return cluster.CreateSession()
}

func partitionCounter(ctx context.Context, row map[string]interface{}) (interface{}, error) {
	val := uint64(1)
	return val, nil
}

func main() {
	if _, err := flags.Parse(&opts); err != nil {
		panic(err)
	}
	session, err := getScyllaClient(opts.Hosts, opts.Username, opts.Password)
	if err != nil {
		panic(err)
	}
	scanner := scanner.NewBasicScanner(session, partitionCounter, opts.Keyspace, opts.Table, opts.PartitionKey)
	scanner.Scan(opts.NumWorkers)
}
