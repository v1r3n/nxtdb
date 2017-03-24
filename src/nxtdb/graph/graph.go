package graph

//Graph interface
type Graph interface {

	//Graph Store Management APIs

	//Open the underlying graph store
	Open()

	//Close. Should be called before exiting the program
	Close()

	//Transaction management
	Tx() Transaction

	//Misc
	NewProperty(key string, value []byte) Property
}