package rocksdb

import (
	//"log"
	"bytes"
)

type PropMergeOp struct {

}

func (m PropMergeOp) FullMerge(key []byte, existingValue []byte, operands [][]byte) ([]byte, bool) {
	//log.Println("\t\t-->FullMerge", string(key))
	buf := new(bytes.Buffer)
	buf.Write(existingValue)

	for i := 0; i < len(operands); i++ {
		op := operands[i]
		buf.Write(op)
	}

	return buf.Bytes(), true
}

func (m PropMergeOp) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	//log.Println("\t\t-->PartialMerge", string(key), string(leftOperand), string(rightOperand))
	buf := new(bytes.Buffer)
	buf.Write(leftOperand)
	buf.Write(rightOperand)
	return buf.Bytes(), true
}

func (m PropMergeOp) Name() string {
	return "PropertyMergeOperator"
}