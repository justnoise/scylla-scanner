module github.com/justnoise/scylla-scanner

go 1.22.7

require (
	github.com/gocql/gocql v1.7.0
	github.com/justnoise/parallel v0.0.0-20240726180806-f20361bef304
)

require (
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.14.5
