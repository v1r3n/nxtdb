package graph

type Vertex interface {
	Id() string
	Property(name string) []byte
	Label() string
}

type VertexProperty interface {
	Key() string
	Value() []byte
}

type Edge interface {
	Label() string
	From() *VertexIterator
	To() *VertexIterator
}