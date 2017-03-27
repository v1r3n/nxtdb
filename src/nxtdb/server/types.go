package server

import "nxtdb/graph"

type CommandHandler func(cmd *Command, store *GraphStore) ([][]byte, error)

type GraphStore struct {
	path string
	graph *graph.Graph
	handlers map[string]CommandHandler
}