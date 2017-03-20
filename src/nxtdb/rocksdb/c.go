package rocksdb

/*
#cgo LDFLAGS: -L/usr/local/Cellar/rocksdb/5.1.4/lib -lrocksdb
#cgo CFLAGS: -I/usr/local/Cellar/rocksdb/5.1.4/include
#include "rocksdb/c.h"
#include <stdlib.h>
*/
import "C"
import (
	"unsafe"
)


type Options struct {
	Opt *C.rocksdb_options_t
}

func (o *Options) SetCreateIfMissing(b bool) {
	C.rocksdb_options_set_create_if_missing(o.Opt, boolToUchar(b))
}

type ReadOptions struct {
	Opt *C.rocksdb_readoptions_t
}

type WriteOptions struct {
	Opt *C.rocksdb_writeoptions_t
}

func NewOptions() *Options {
	opt := C.rocksdb_options_create()
	return &Options{opt}
}

func NewReadOptions() *ReadOptions {
	opt := C.rocksdb_readoptions_create()
	return &ReadOptions{opt}
}

func NewWriteOptions() *WriteOptions {
	opt := C.rocksdb_writeoptions_create()
	return &WriteOptions{opt}
}

type DatabaseError string

func (err DatabaseError) Error() string {
	return string(err)
}

type DB struct {
	Ldb *C.rocksdb_t
}

func Open(dbname string, o *Options) (*DB, error) {
	var errStr *C.char
	ldbname := C.CString(dbname)
	defer C.free(unsafe.Pointer(ldbname))

	rocksdb := C.rocksdb_open(o.Opt, ldbname, &errStr)
	if errStr != nil {
		gs := C.GoString(errStr)
		C.rocksdb_free(unsafe.Pointer(errStr))
		return nil, DatabaseError(gs)
	}
	return &DB{rocksdb}, nil
}

func (db *DB) Put(wo *WriteOptions, key, value []byte) error {
	var errStr *C.char
	// rocksdb_put, _get, and _delete call memcpy() (by way of Memtable::Add)
	// when called, so we do not need to worry about these []byte being
	// reclaimed by GC.
	var k, v *C.char
	if len(key) != 0 {
		k = (*C.char)(unsafe.Pointer(&key[0]))
	}
	if len(value) != 0 {
		v = (*C.char)(unsafe.Pointer(&value[0]))
	}

	lenk := len(key)
	lenv := len(value)
	C.rocksdb_put(
		db.Ldb, wo.Opt, k, C.size_t(lenk), v, C.size_t(lenv), &errStr)

	if errStr != nil {
		gs := C.GoString(errStr)
		C.rocksdb_free(unsafe.Pointer(errStr))
		return DatabaseError(gs)
	}
	return nil
}
func (db *DB) NewIterator(ro *ReadOptions) *Iterator {
	it := C.rocksdb_create_iterator(db.Ldb, ro.Opt)
	return &Iterator{Iter: it}
}
func (db *DB) Get(ro *ReadOptions, key []byte) ([]byte, error) {
	var errStr *C.char
	var vallen C.size_t
	var k *C.char
	if len(key) != 0 {
		k = (*C.char)(unsafe.Pointer(&key[0]))
	}

	value := C.rocksdb_get(
		db.Ldb, ro.Opt, k, C.size_t(len(key)), &vallen, &errStr)

	if errStr != nil {
		gs := C.GoString(errStr)
		C.rocksdb_free(unsafe.Pointer(errStr))
		return nil, DatabaseError(gs)
	}

	if value == nil {
		return nil, nil
	}

	defer C.rocksdb_free(unsafe.Pointer(value))
	return C.GoBytes(unsafe.Pointer(value), C.int(vallen)), nil
}



