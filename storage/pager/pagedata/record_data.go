package pagedata

import (
	"bytes"
	"encoding/gob"
	"go-database/parser/ast"
	"io"
)

type RecordData struct {
	rows []ast.Row
}

func NewRecordData() *RecordData {
	return &RecordData{}
}

func (record *RecordData) Encode() []byte {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(record)
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

func (record *RecordData) Decode(r io.Reader) error {
	decoder := gob.NewDecoder(r)
	return decoder.Decode(record)
}

func (record *RecordData) AppendData(rows ast.Row) {
	record.rows = append(record.rows, rows)
}

func (record *RecordData) Size() int {
	return len(record.Encode())
}

func (record *RecordData) Rows() []ast.Row {
	return record.rows
}