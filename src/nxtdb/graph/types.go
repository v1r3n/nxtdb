package graph

//Schema Label.  Both Vertex and Edges are Labeled
type Label interface {
	Name() string
	Id() string
}

//A graph vertex
type Vertex interface {
	Id() string
	Property(name string) []byte
	Properties() []Property
	Label() Label
}

//Directional Edge between two vertices
type Edge interface {
	Label() Label
	From() Vertex
	To() Vertex
}

//Key, Value
type Property interface {
	Key() string
	Value() []byte
}