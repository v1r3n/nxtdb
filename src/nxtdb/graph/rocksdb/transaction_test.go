package rocksdb_test

import (
	"testing"
	"nxtdb/graph/rocksdb"
)

func TestGraphTransaction(t *testing.T) {
	t.Log("Hello World")
	tx := rocksdb.NewTransaction(nil)

	t.Log("Transaction", tx)


}