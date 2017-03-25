package rocksdb

/*
#cgo LDFLAGS: -L/usr/local/Cellar/rocksdb/5.1.4/lib -lrocksdb
#cgo CFLAGS: -I/usr/local/Cellar/rocksdb/5.1.4/include
#include "rocksdb/c.h"
#include <stdlib.h>
*/
import (
	grocks "github.com/v1r3n/gorocksdb"
	"log"
	. "nxtdb/graph"
)

var sharedDB Graph;

type RocksDBGraph struct {
	dbPath       string
	db           *grocks.DB
	cfhVtx       *grocks.ColumnFamilyHandle
	cfhIndx      *grocks.ColumnFamilyHandle
	cfhEdge      *grocks.ColumnFamilyHandle
	transactions map[string]*GraphTransaction
	opened       bool
}

func OpenGraphDb(path string) Graph {

	if sharedDB == nil {
		sharedDB = &RocksDBGraph{
			dbPath:path,
		}
		sharedDB.Open()
	}

	return sharedDB
}

func (graphdb *RocksDBGraph) Open() {

	if graphdb.opened {
		return
	}

	options := grocks.NewDefaultOptions()
	options.SetCreateIfMissing(true)
	options.SetCreateIfMissingColumnFamilies(true)
	options.SetMergeOperator(PropMergeOp{})
	db, cfh, err := grocks.OpenDbColumnFamilies(options, graphdb.dbPath,
		[]string{"default", "vertex", "edge", "index"},
		[]*grocks.Options{options, options, options, options},
	)
	if err != nil {
		log.Fatal("Cannot open the database", err)
		return
	}

	graphdb.db = db
	graphdb.cfhVtx = cfh[1]
	graphdb.cfhEdge = cfh[2]
	graphdb.cfhIndx = cfh[3]
	graphdb.transactions = make(map[string]*GraphTransaction)
	graphdb.opened = true

	return;
}

func (db *RocksDBGraph) Close() {
	db.cfhVtx.Destroy()
	db.cfhEdge.Destroy()
	db.cfhIndx.Destroy()
	db.db.Close()
}

func (db *RocksDBGraph) Tx() Transaction {
	tx := NewGraphTransaction(db.db, db.cfhVtx, db.cfhIndx, db.cfhEdge)
	return tx
}

func (db *RocksDBGraph) NewProperty(key string, value []byte) Property {
	return GraphProperty{
		key : key,
		value : value,
	}
}



