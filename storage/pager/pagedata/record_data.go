package pagedata

import (
	"bytes"
	"encoding/binary"
	"go-database/parser/ast"
	"io"
)

type RecordData struct {
	rows []*ast.Row
}

func NewRecordData() *RecordData {
	return &RecordData{
		rows: make([]*ast.Row, 0),
	}
}

func (record *RecordData) Encode() []byte {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, len(record.rows))
	for _, row := range record.rows {
		buff.Write(row.Encode())
	}
	return buff.Bytes()
}

func (record *RecordData) Decode(r io.Reader) error {
	var len int
	binary.Read(r, binary.BigEndian, &len)
	record.rows = make([]*ast.Row, len)
	for i := range record.rows {
		row := &ast.Row{}
		row.Decode(r)
		record.rows[i] = row
	}
	return nil
}

// @description: add new rows into table
func (record *RecordData) AppendData(row *ast.Row) {
	record.rows = append(record.rows, row)
}

func (record *RecordData) Size() int {
	return len(record.Encode())
}

func (record *RecordData) Rows() []*ast.Row {
	return record.rows
}
