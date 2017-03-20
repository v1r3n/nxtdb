package nxtdb

type DuplicateVertexError struct {
	message string
}

type NoSuchVertexError struct {
	message string
}

func NewDuplicateVertexError(message string) *DuplicateVertexError {
	return &DuplicateVertexError {
		message:message,
	}
}

func NewNoSuchVertex(message string) *NoSuchVertexError {
	return &NoSuchVertexError{
		message:message,
	}
}

//Iterator for the vertex get operations
type VertexIterator interface {
	Next() *Vertex
	HasNext() bool
	Close()
}

type Graph interface {

	//Management
	Open()
	Close()

	//Indexing
	CreateIndex(string label, propertyKey string)

	//Create
	Add(vertex *Vertex) (string, DuplicateVertexError)
	AddProperty(id string, key string, value []byte) NoSuchVertexError
	AddProperties(id string, map[string][]byte) NoSuchVertexError
	AddEdge(from string, to string, label string) NoSuchVertexError

	//Remove
	RemoveVertex(id string) NoSuchVertexError
	RemoveProperty(id string, key string) NoSuchVertexError
	RemoveEdge(from string, to string, label string) NoSuchVertexError

	//Read operations
	GetVertex(id string) *Vertex
	GetVerticesByLabel(vertexLabel string) *VertexIterator
	GetVertices(id string, edgeLabel string, outgoing bool) (*VertexIterator, NoSuchVertexError)
	CountVertices(vertexLabel string) uint64
}