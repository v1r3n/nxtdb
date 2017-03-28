/*
 Interface for working with the graph database
 */
package graph

//Graph interface
type Graph interface {

	//Open the underlying graph store
	Open()

	//Close. Should be called before exiting the program
	Close()

	//Begins the new transaction
	Tx() Transaction

	GetTx(txId string) Transaction

	//Misc
	NewProperty(key string, value []byte) Property

	//Create an index for the vertex property
	IndexProperty(property string)
}