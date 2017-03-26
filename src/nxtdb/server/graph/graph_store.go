/*
graphstore provides RESP based command execution for the graph db
 Supported commands:

 Add label <label name>
 	returns: OK
 Add Vertex <label name> <prop1> <value1> <prop2> <value2> ...
	returns: vertex id
 */
package graphstore

import (
	. "nxtdb/server"
	rocksgraph "nxtdb/graph/rocksdb"
	"nxtdb/graph"
	"log"
	"strconv"
	"strings"
)

type CommandHandler func(cmd *Command, store *GraphStore) ([][]byte, error)

type GraphStore struct {
	path string
	graph *graph.Graph
	handlers map[string]CommandHandler
}


func (store GraphStore) ExecuteCommand(cmd *Command) ([][]byte, error) {
	bytes, err := execute(*cmd, &store)
	return bytes, err
}

func execute(cmd Command, store *GraphStore) ([][]byte, error) {
	log.Println("Command ", cmd.Cmd)
	for indx, arg := range cmd.Args {
		log.Println("arg[" + strconv.Itoa(indx) + "]", string(arg))
	}
	handler := store.handlers[cmd.Cmd]
	if handler != nil {
		return handler(&cmd, store)
	}

	return nil, nil
}

func New(path string) Store {
	var g = rocksgraph.OpenGraphDb("./graph.db")
	handlers := make(map[string]CommandHandler)
	store := GraphStore{path, &g, handlers}
	store.handlers["ADD"] = add
	store.handlers["GET"] = get
	store.handlers["COMMAND"] = command
	return store
}


//private functions
func add(cmd *Command, store *GraphStore) ([][]byte, error) {

	what := string(cmd.Args[0])
	target := strings.ToUpper(what)
	log.Println("target", target)

	if strings.EqualFold(target, "LABEL") {

		label := cmd.Args[1]
		store.addLabel(string(label))
		return ok("OK"), nil

	} else if strings.EqualFold(target, "VERTEX") {

	}

	bytes := make([][]byte, 1)
	bytes[0] = []byte("Not supported")
	return bytes, nil
}

func get(cmd *Command, store *GraphStore) ([][]byte, error) {

	what := string(cmd.Args[0])
	target := strings.ToUpper(what)
	log.Println("target", target)

	if strings.EqualFold(target, "LABEL") {

		label := cmd.Args[1]
		graph := store.graph
		tx := (*graph).Tx()
		found := tx.GetLabel(string(label))
		if found != nil {
			bytes := make([][]byte, 1)
			bytes[0] = []byte("Id:" + found.Id() + ", name:" + found.Name())
			return bytes, nil
		} else {
			bytes := make([][]byte, 1)
			bytes[0] = []byte("Not found")
			return bytes, nil
		}

	} else if strings.EqualFold(target, "VERTEX") {

	}

	bytes := make([][]byte, 1)
	bytes[0] = []byte("Not supported")
	return bytes, nil
}

func command(cmd *Command, store *GraphStore) ([][]byte, error) {
	return ok("Hello"), nil
}
func (store *GraphStore) addLabel(label string) {
	graph := store.graph
	tx := (*graph).Tx()
	tx.AddLabel(label)
	tx.Commit()
}

func ok(msg string) [][]byte {
	bytes := make([][]byte, 1)
	bytes[0] = []byte(msg)
	return bytes
}