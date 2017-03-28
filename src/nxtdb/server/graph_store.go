/*
graphstore provides RESP based command execution for the graph db
 Supported commands:

 Add label <label name>
 	returns: OK
 Add Vertex <label name> <prop1> <value1> <prop2> <value2> ...
	returns: vertex id
 */
package server

import (
	rocksgraph "nxtdb/graph/rocksdb"
	"nxtdb/graph"
	"log"
	"errors"
	"encoding/json"
)

type CommandHandler func(cmd *Command, store *GraphStore) ([][]byte, error)

type GraphStore struct {
	path string
	graph *graph.Graph
	handlers map[string]CommandHandler
	transactions map[string]graph.Transaction
}

func NewGraphStore(path string) Store {

	var g = rocksgraph.OpenGraphDb("./graph.db")

	handlers := make(map[string]CommandHandler)
	store := GraphStore{path, &g, handlers, make(map[string]graph.Transaction)}

	store.handlers["ADD_LABEL"] = addLabel
	store.handlers["GET_LABEL"] = getLabel
	store.handlers["ADD_VERTEX"] = addVertex
	store.handlers["GET_VERTEX"] = getVertex
	store.handlers["COMMIT"] = commitTx
	store.handlers["COMMAND"] = command
	return store
}

func (store GraphStore) ExecuteCommand(cmd *Command) ([][]byte, error) {
	bytes, err := execute(*cmd, &store)
	return bytes, err
}

func execute(cmd Command, store *GraphStore) ([][]byte, error) {
	log.Println("Command ", cmd.Cmd)
	handler := store.handlers[cmd.Cmd]
	if handler != nil {
		return handler(&cmd, store)
	}
	return nil, errors.New("Unsupported command: " + cmd.Cmd)

}


func (store *GraphStore) getTx(sessionId string) graph.Transaction {
	tx := store.transactions[sessionId]
	if tx == nil {
		tx = (*store.graph).Tx()
		store.transactions[sessionId] = tx
	}
	return tx
}

//private functions

func commitTx(cmd *Command, store *GraphStore) ([][]byte, error) {
	tx := store.transactions[cmd.SessionId]
	if tx != nil {
		tx.Commit()
		delete(store.transactions, cmd.SessionId)
	}
	return ok("OK"), nil
}


//add_label <name>
func addLabel(cmd *Command, store *GraphStore) ([][]byte, error) {
	if len(cmd.Args) != 1 {
		return nil, errors.New("missing label name")
	}
	label := cmd.Args[0]
	store.addLabel(string(label), cmd.SessionId)
	return ok("OK"), nil
}

//get_label <name>
func getLabel(cmd *Command, store *GraphStore) ([][]byte, error) {

	if len(cmd.Args) != 1 {
		return nil, errors.New("missing label name")
	}
	label := cmd.Args[0]
	tx := store.getTx(cmd.SessionId)
	found := tx.GetLabel(string(label))
	if found != nil {
		bytes := make([][]byte, 1)
		bytes[0] = []byte("Id:" + found.Id() + ", name:" + found.Name())
		return bytes, nil
	}
	return nil, errors.New("Label not found")
}

//add_vertex <label> [key, value]...
func addVertex(cmd *Command, store *GraphStore) ([][]byte, error) {
	if len(cmd.Args) < 1 {
		return nil, errors.New("add_vertex <label> [key, value]...")
	}
	label := string(cmd.Args[0])
	tx := store.getTx(cmd.SessionId)
	g := (*store.graph)
	properties := make([]graph.Property, 0)
	for i := 1; i < len(cmd.Args) - 1; i++ {
		propKey := cmd.Args[i]
		propValue := cmd.Args[i+1]
		property := g.NewProperty(string(propKey), propValue)
		properties = append(properties, property)
		i++
	}
	lbl := tx.GetLabel(label)
	if lbl == nil {
		return nil, errors.New("No such label " + label)
	}
	vtx := tx.Add(lbl, properties...)
	return ok(vtx.Id()), nil
}

func getVertex(cmd *Command, store *GraphStore) ([][]byte, error) {
	if len(cmd.Args) != 1 {
		return nil, errors.New("get_vertex id")
	}
	id := string(cmd.Args[0])
	tx := store.getTx(cmd.SessionId)
	found := tx.GetVertex(id)
	if found == nil {
		return nil, errors.New("no vertex found by id: " + id)
	}
	jsonMap := make(map[string]interface{})
	jsonMap["Id"] = id
	jsonMap["Label"] = found.Label().Name()

	properties := make(map[string]string)
	for _, prop := range found.Properties() {
		properties[prop.Key()] = string(prop.Value())
	}
	jsonMap["Properties"] = properties
	jsonBytes, err := json.Marshal(jsonMap)
	if err != nil {
		return nil, err
	}
	bytes := make([][]byte, 1)
	bytes[0] = jsonBytes
	return bytes, nil
}

func command(cmd *Command, store *GraphStore) ([][]byte, error) {
	return ok("Hello"), nil
}
func (store *GraphStore) addLabel(label string, sessionId string) {
	tx := store.getTx(sessionId)
	tx.AddLabel(label)
}

func ok(msg string) [][]byte {
	bytes := make([][]byte, 1)
	bytes[0] = []byte(msg)
	return bytes
}