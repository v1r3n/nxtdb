package graph

type Transaction interface {
	//Commit Transaction to the store
	Commit() error

	//Rollback any changes done
	Rollback()
	
	//Add a new label
	AddLabel(label string) Label

	//Get the label
	GetLabel(label string) Label

	//Create
	Add(label Label, properties...Property) string
	AddProperty(id string, key string, value []byte)
	AddProperties(id string, properties...Property)
	AddEdge(from string, to string, label Label)

	//Remove
	RemoveVertex(id string)
	RemoveProperty(id string, key string)
	RemoveEdge(from string, to string, label Label)

	//Read operations
	GetVertex(id string) Vertex
	GetVerticesByLabel(vertexLabel Label) VertexIterator
	GetVertices(id string, edgeLabel Label, outgoing bool) VertexIterator

}