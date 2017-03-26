/**

 */
package graph

//Schema Label.  Both Vertex and Edges are Labeled
type Label interface {
	Name() string
	Id() string
}

//A graph vertex
type Vertex interface {
	//Id of the vertex
	Id() string

	//Get the property
	Property(name string) []byte

	//Get all the properties associated with the vertex
	Properties() []Property

	//Vertex Label
	Label() Label

	//Iterator for all the outgoing vertices connected by specified label
	Out(label Label) VertexIterator

	//Iterator for all the incoming vertices connected by specified label
	In(label Label) VertexIterator
}

//Directional Edge between two vertices
type Edge interface {
	Label() Label
	From() string
	To() string
}

//Key, Value
type Property interface {
	Key() string
	Value() []byte
}

type Config struct {
	//use supplied vertex ids
	//batch loading
	//sync
	//quorum
}