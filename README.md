# Scylla Scanner

scylla-scanner is a flexible library to implement efficient parallel full scans of a scylla table.

## Background

* https://www.scylladb.com/2017/02/13/efficient-full-table-scans-with-scylla-1-6/
* https://github.com/scylladb/scylla-code-samples/blob/master/efficient_full_table_scan_example_code/efficient_full_table_scan.go

## Usage

The scylla-scanner library is designed to be flexible, however that flexibility comes at the expense of some complexity.  The goal was to create a library that will scan through a scylla table in parallel and allow the user to customize:

1. The query that will be used to scan the table.
2. A function to call for every row returned in the scan.
3. A structure that will aggregate the results of the scan.

Overkill?  Probably.  Fun?  Definately!

### Basic Usage

main.go shows the most basic use case for the library.  A `BasicScanner` is created and passed a callback (`partitionCounter`) that returns 1 for every row the scanner produces.  The default result aggregator (`IntResult) sums all the results from partitionCounter and prints the number of partitions counted.

A custom `PartitionCallback` can be used with the default `QueryBuilder` and `IntResult` returned by `NewBasicScanner` to count the number of unwanted partitions detected in the table

```go
badPartitionKeys := map[string]struct{}{
	"bad":      {},
	"no_good":  {},
	"horrible": {},
}

badPartitionKeyCounter := func (ctx context.Context, row map[string]interface{}) (interface{}, error) {
	if _, ok := badPartitionKeys[row[opts.PartitionKey].(string)]; ok {
		return uint64(1), fmt.Errorf("Bad partition key: %s", row[opts.PartitionKey].(string))
	}
	return uint64(0), nil
}
scanner := scanner.NewBasicScanner(session, badPartitionCounter, opts.Keyspace, opts.Table, opts.PartitionKey)
scanner.Scan(opts.NumWorkers)
```

### Advanced Usage

It's possible to supply a custom `QueryBuilder`, `PartitionCallback` and `ResultFactory` to `NewScanner` to change how those portions of the library behave.

As an example, we might want to capture all partitions in the database that have a row with a created_at date before a certain time.  This requires a custom query to generate rows.

```go
type CreatedAtQueryBuilder struct {
	Keyspace     string
	Table        string
	PartitionKey string
}

func (q *CreatedAtQueryBuilder) Build() string {
	queryTemplate := "SELECT token(%s), %s, created_at FROM %s.%s WHERE token(%s) >= ? AND token(%s) <= ?"
	return fmt.Sprintf(queryTemplate, q.PartitionKey, q.PartitionKey, q.Keyspace, q.Table, q.PartitionKey, q.PartitionKey)
}

func oldCreatedAtCounter(ctx context.Context, row map[string]interface{}) (interface{}, error) {
	if row["created_at"].(time.Time).Before(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)) {
		return 1, fmt.Errorf("Old created_at: %s", row["created_at"].(time.Time))
	}
	return 0, nil
}
scanner := scanner.NewScanner(session, oldCreatedAt, createdAtQueryBuilder, NewIntResult)
scanner.Scan(opts.NumWorkers)
```
