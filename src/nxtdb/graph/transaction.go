package graph

//Common Graph Operations
type GraphOps interface {

	//Add a new label
	AddLabel(label string) Label

	//Get the label
	GetLabel(label string) Label

	//Create Operations
	Add(label Label, properties...Property) Vertex
	AddEdge(from string, to string, label Label) Edge

	//Update & Delete properties for Vertex
	SetProperty(id string, key string, value []byte)
	SetProperties(id string, properties...Property)

	RemoveProperty(id string, key string)
	RemoveProperties(id string, key...string)


	//Remove
	RemoveVertex(id string)

	RemoveEdge(from string, to string, label Label)

	//Read operations
	GetVertex(id string) Vertex
	GetVerticesByLabel(vertexLabel Label) VertexIterator
	GetVertices(id string, edgeLabel Label, outgoing bool) VertexIterator

}

//Graph Transactions
type Transaction interface {

	//Transaction Id
	Id() string

	//Commits the transaction
	Commit() error

	//Rollback the transaction
	Rollback()

	GraphOps
}