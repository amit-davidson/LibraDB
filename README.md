# LibraDB

[![made-with-Go](https://github.com/go-critic/go-critic/workflows/Go/badge.svg)](http://golang.org)
[![made-with-Go](https://img.shields.io/badge/Made%20with-Go-1f425f.svg)](http://golang.org)
[![MIT license](https://img.shields.io/badge/License-MIT-blue.svg)](https://lbesson.mit-license.org/)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](http://makeapullrequest.com)
[![amit-davidson](https://circleci.com/gh/amit-davidson/LibraDB.svg?style=svg)](https://app.circleci.com/pipelines/github/amit-davidson/LibraDB)

LibraDB is a simple, persistent key/value store written in pure Go. The project aims to provide a working yet simple
example of a working database. If you're interested in databases, I encourage you to start here.

## Installing

To start using LibraDB, install Go and run `go get`:

```sh
go get -u github.com/amit-davidson/LibraDB
```

## Basic usage
```go
package main

import "github.com/amit-davidson/LibraDB"

func main() {
	path := "libra.db"
	db, _ := LibraDB.Open(path, LibraDB.DefaultOptions)

	tx := db.WriteTx()
	name := []byte("test")
	collection, _ := tx.CreateCollection(name)

	key, value := []byte("key1"), []byte("value1")
	_ = collection.Put(key, value)

	_ = tx.Commit()
}
```
## Transactions
Read-only and read-write transactions are supported. LibraDB allows multiple read transactions and one read-write 
transaction at the same time. Transactions are goroutine-safe.

LibraDB has an isolation level: [Read committed](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Read_committed).
In simpler words, it restricts the reader from seeing any intermediate, uncommitted, 'dirty' changes.

### Read-write transactions

```go
tx := db.WriteTx()
...
if err := tx.Commit(); err != nil {
    return err
}
```
### Read-only transactions
```go
tx := db.ReadTx()
...
if err := tx.Commit(); err != nil {
    return err
}
```

## Collections
Collections are a grouping of key-value pairs. Collections are used to organize and quickly access data as each
collection is B-Tree by itself. All keys in a collection must be unique.
```go
tx := db.WriteTx()
collection, err := tx.CreateCollection([]byte("test"))
if err != nil {
	return err
}
_ = tx.Commit()
```

### Auto generating ID
The `Collection.ID()` function returns an integer to be used as a unique identifier for key/value pairs.
```go
tx := db.WriteTx()
collection, err := tx.GetCollection([]byte("test"))
if err != nil {
    return err
}
id := collection.ID()
_ = tx.Commit()
```
## Key-Value Pairs
Key/value pairs reside inside collections. CRUD operations are possible using the methods `Collection.Put` 
`Collection.Find` `Collection.Remove` as shown below.   
```go
tx := db.WriteTx()
collection, err := tx.GetCollection([]byte("test"))
if  err != nil {
    return err
}

key, value := []byte("key1"), []byte("value1")
if err := collection.Put(key, value); err != nil {
    return err
}
if item, err := collection.Find(key); err != nil {
    return err
}

if err := collection.Remove(key); err != nil {
    return err
}
_ = tx.Commit()
```