package rocksdb_test

import (
	"testing"
	"nxtdb/graph/rocksdb"
	"nxtdb/graph"
)

func TestGraphTransaction(t *testing.T) {
	t.Log("Hello World")
	var tx graph.Transaction = rocksdb.NewTransaction(nil)

	t.Log("Transaction", tx.Id())


}