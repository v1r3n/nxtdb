package graph

//Iterator for the vertex get operations
type VertexIterator interface {
	Next() Vertex
	HasNext() bool
	Close()
}