package rocksdb

import (
	"bytes"
)

type PropMergeOp struct {

}

func (m PropMergeOp) FullMerge(key []byte, existingValue []byte, operands [][]byte) ([]byte, bool) {
	buf := new(bytes.Buffer)
	buf.Write(existingValue)

	for i := 0; i < len(operands); i++ {
		op := operands[i]
		buf.Write(op)
	}

	return buf.Bytes(), true
}

func (m PropMergeOp) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	buf := new(bytes.Buffer)
	buf.Write(leftOperand)
	buf.Write(rightOperand)
	return buf.Bytes(), true
}

func (m PropMergeOp) Name() string {
	return "PropertyMergeOperator"
}