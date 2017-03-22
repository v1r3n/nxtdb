package graph

type DuplicateVertexError struct {
	message string
}

type NoSuchVertexError struct {
	message string
}

func NewDuplicateVertexError(message string) DuplicateVertexError {
	return DuplicateVertexError {
		message:message,
	}
}

func NewNoSuchVertex(message string) NoSuchVertexError {
	return NoSuchVertexError{
		message:message,
	}
}

//Iterator for the vertex get operations
type VertexIterator interface {
	Next() Vertex
	HasNext() bool
	Close()
}

//Graph interface
type Graph interface {

	//Graph Store Management APIs

	//Open the underlying graph store
	Open()

	//Close. Should be called before exiting the program
	Close()

	//Schema Management APIs
	//Add a new label
	AddLabel(label string) Label

	//Get the label
	GetLabel(label string) Label

	//Renames an existing label
	//RenameLabel(label Label, newName string)

	//Create
	Add(label Label, properties...Property) string
	AddProperty(id string, key string, value []byte)
	AddProperties(id string, properties map[string][]byte)
	AddEdge(from string, to string, label Label)

	//Remove
	RemoveVertex(id string)
	RemoveProperty(id string, key string)
	RemoveEdge(from string, to string, label string)

	//Read operations
	GetVertex(id string) Vertex
	GetVerticesByLabel(vertexLabel Label) VertexIterator
	GetVertices(id string, edgeLabel Label, outgoing bool) VertexIterator

	//Transaction management
	BeginTransaction() Transaction

}