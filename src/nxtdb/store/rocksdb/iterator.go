package rocksdb

import (
	grocks "github.com/v1r3n/gorocksdb"
	. "nxtdb/graph"
)

type GraphVertexIterator struct {
	prefix   []byte
	iterator *grocks.Iterator
	tx       *GraphTransaction
}

func NewGraphVertexIterator(prefix []byte, iterator *grocks.Iterator, tx *GraphTransaction) *GraphVertexIterator {
	gvi := GraphVertexIterator{
		prefix: prefix,
		iterator: iterator,
		tx: tx,
	}
	gvi.open()
	return &gvi
}

func (it *GraphVertexIterator) Next() Vertex {
	if it.iterator.ValidForPrefix(it.prefix) {
		id := string(it.iterator.Value().Data())
		it.iterator.Next()
		return it.tx.GetVertex(id)
	}
	return nil
}
func (it *GraphVertexIterator) HasNext() bool {
	return it.iterator.ValidForPrefix(it.prefix)
}
func (it *GraphVertexIterator) Close() {
	it.iterator.Close()
}

func (it *GraphVertexIterator) open() {
	it.iterator.Seek(it.prefix)
}