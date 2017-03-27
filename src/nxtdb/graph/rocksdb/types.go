package rocksdb

import (
	. "nxtdb/graph"
)

type GraphLabel struct {
	label string
	id    string
}

type GraphProperty struct {
	Name   string
	Val []byte
}

type GraphEdge struct {
	label Label
	from  string
	to    string
}

func (label GraphLabel) Name() string {
	return label.label
}

func (label GraphLabel) Id() string {
	return label.id
}



func (prop GraphProperty) Key() string {
	return prop.Name
}

func (prop GraphProperty) Value() []byte {
	return prop.Val
}

func (edge GraphEdge) Label() Label {
	return edge.label
}

func (edge GraphEdge) From() string {
	return edge.from
}

func (edge GraphEdge) To() string {
	return edge.to
}
