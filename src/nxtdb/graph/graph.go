package graph

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
	CreateIndex(label string, propertyKey string)

	//Create
	Add(vertex *Vertex) string
	AddProperty(id string, key string, value []byte)
	AddProperties(id string, properties map[string][]byte)
	AddEdge(from string, to string, label string)

	//Remove
	RemoveVertex(id string)
	RemoveProperty(id string, key string)
	RemoveEdge(from string, to string, label string)

	//Read operations
	GetVertex(id string) *Vertex
	GetVerticesByLabel(vertexLabel string) *VertexIterator
	GetVertices(id string, edgeLabel string, outgoing bool) *VertexIterator
	CountVertices(vertexLabel string) uint64
}