package rocksdb

// #cgo LDFLAGS: -lrocksdb
// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"

import (
	"unsafe"
)

type IteratorError string

func (e IteratorError) Error() string {
	return string(e)
}


type Iterator struct {
	Iter *C.rocksdb_iterator_t
}

func (it *Iterator) Valid() bool {
	return ucharToBool(C.rocksdb_iter_valid(it.Iter))
}

// Key returns a copy the key in the database the iterator currently holds.
//
// If Valid returns false, this method will panic.
func (it *Iterator) Key() []byte {
	var klen C.size_t
	kdata := C.rocksdb_iter_key(it.Iter, &klen)
	if kdata == nil {
		return nil
	}
	// Unlike DB.Get, the key, kdata, returned is not meant to be freed by the
	// client. It's a direct reference to data managed by the iterator_t
	// instead of a copy.  So, we must not free it here but simply copy it
	// with GoBytes.
	return C.GoBytes(unsafe.Pointer(kdata), C.int(klen))
}

// Value returns a copy of the value in the database the iterator currently
// holds.
//
// If Valid returns false, this method will panic.
func (it *Iterator) Value() []byte {
	var vlen C.size_t
	vdata := C.rocksdb_iter_value(it.Iter, &vlen)
	if vdata == nil {
		return nil
	}
	// Unlike DB.Get, the value, vdata, returned is not meant to be freed by
	// the client. It's a direct reference to data managed by the iterator_t
	// instead of a copy. So, we must not free it here but simply copy it with
	// GoBytes.
	return C.GoBytes(unsafe.Pointer(vdata), C.int(vlen))
}

// Next moves the iterator to the next sequential key in the database, as
// defined by the Comparator in the ReadOptions used to create this Iterator.
//
// If Valid returns false, this method will panic.
func (it *Iterator) Next() {
	C.rocksdb_iter_next(it.Iter)
}

// Prev moves the iterator to the previous sequential key in the database, as
// defined by the Comparator in the ReadOptions used to create this Iterator.
//
// If Valid returns false, this method will panic.
func (it *Iterator) Prev() {
	C.rocksdb_iter_prev(it.Iter)
}

// SeekToFirst moves the iterator to the first key in the database, as defined
// by the Comparator in the ReadOptions used to create this Iterator.
//
// This method is safe to call when Valid returns false.
func (it *Iterator) SeekToFirst() {
	C.rocksdb_iter_seek_to_first(it.Iter)
}

// SeekToLast moves the iterator to the last key in the database, as defined
// by the Comparator in the ReadOptions used to create this Iterator.
//
// This method is safe to call when Valid returns false.
func (it *Iterator) SeekToLast() {
	C.rocksdb_iter_seek_to_last(it.Iter)
}

// Seek moves the iterator the position of the key given or, if the key
// doesn't exist, the next key that does exist in the database. If the key
// doesn't exist, and there is no next key, the Iterator becomes invalid.
//
// This method is safe to call when Valid returns false.
func (it *Iterator) Seek(key []byte) {
	C.rocksdb_iter_seek(it.Iter, (*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)))
}

// GetError returns an IteratorError from RocksDB if it had one during
// iteration.
//
// This method is safe to call when Valid returns false.
func (it *Iterator) GetError() error {
	var errStr *C.char
	C.rocksdb_iter_get_error(it.Iter, &errStr)
	if errStr != nil {
		gs := C.GoString(errStr)
		C.rocksdb_free(unsafe.Pointer(errStr))
		return IteratorError(gs)
	}
	return nil
}

// Close deallocates the given Iterator, freeing the underlying C struct.
func (it *Iterator) Close() {
	C.rocksdb_iter_destroy(it.Iter)
	it.Iter = nil
}