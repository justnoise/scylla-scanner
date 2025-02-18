package scanner

import "fmt"

type QueryBuilder interface {
	Build() string
}

type BasicQueryBuilder struct {
	Keyspace     string
	Table        string
	PartitionKey string
}

func (q *BasicQueryBuilder) Build() string {
	queryTemplate := "SELECT DISTINCT token(%s), %s FROM %s.%s WHERE token(%s) >= ? AND token(%s) <= ?"
	return fmt.Sprintf(queryTemplate, q.PartitionKey, q.PartitionKey, q.Keyspace, q.Table, q.PartitionKey, q.PartitionKey)
}
