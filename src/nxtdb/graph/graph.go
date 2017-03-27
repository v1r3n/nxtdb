/*
 Interface for working with the graph database
 */
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


//Graph interface
type Graph interface {

	//Open the underlying graph store
	Open()

	//Close. Should be called before exiting the program
	Close()

	//Transaction management
	NewTransaction()

	//Commit current Tx
	CommitTransaction()

	//Rollback current Tx
	RollbackTransaction()

	//Misc
	NewProperty(key string, value []byte) Property

	//Create an index for the vertex property
	IndexProperty(property string)

	GraphOps
}