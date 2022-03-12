package ast

import (
	"encoding/binary"
	"go-database/util"
	"io"
)

type SQLType int

type SQLInt int64
type SQLFloat float64
type SQLString string
type SQLColumn string

const (
	ST_INT SQLType = iota
	ST_FLOAT
	ST_STRING
	ST_COLUMN
	ST_ILLEGAL
)

type SQLValue interface {
	Raw() []byte
	GetType() SQLType
	Encode(w io.Writer)
	Decode(r io.Reader)
}

// INT
func (sqlint *SQLInt) Raw() []byte {
	return util.Int64ToBytes(int64(*sqlint))
}

func (sqlint *SQLInt) GetType() SQLType {
	return ST_INT
}

func (sqlint *SQLInt) Encode(w io.Writer) {
	binary.Write(w, binary.BigEndian, ST_INT)
	binary.Write(w, binary.BigEndian, sqlint)
}

func (sqlint *SQLInt) Decode(r io.Reader) {
	binary.Read(r, binary.BigEndian, sqlint)
}

// FLOAT
func (sqlfloat *SQLFloat) Raw() []byte {
	return util.Float64ToBytes(float64(*sqlfloat))
}

func (sqlfloat *SQLFloat) GetType() SQLType {
	return ST_FLOAT
}

func (sqlfloat *SQLFloat) Encode(w io.Writer) {
	binary.Write(w, binary.BigEndian, ST_FLOAT)
	binary.Write(w, binary.BigEndian, sqlfloat)
}

func (sqlfloat *SQLFloat) Decode(r io.Reader) {
	binary.Read(r, binary.BigEndian, sqlfloat)
}

// STRING
func (sqlstring *SQLString) Raw() []byte {
	return []byte(*sqlstring)
}

func (sqlstring *SQLString) GetType() SQLType {
	return ST_STRING
}

func (sqlstring *SQLString) Encode(w io.Writer) {
	binary.Write(w, binary.BigEndian, ST_STRING)
	binary.Write(w, binary.BigEndian, uint16(len(*sqlstring)))
	w.Write([]byte(*sqlstring))
}

func (sqlstring *SQLString) Decode(r io.Reader) {
	var size uint16
	binary.Read(r, binary.BigEndian, size)
	buff := make([]byte, size)
	r.Read(buff)
	*sqlstring = SQLString(buff)
}

// COLUMN
func (sqlcolumn *SQLColumn) Raw() []byte {
	return []byte(*sqlcolumn)
}

func (sqlcolumn *SQLColumn) GetType() SQLType {
	return ST_COLUMN
}

func (sqlcol *SQLColumn) Encode(w io.Writer) {
	binary.Write(w, binary.BigEndian, ST_COLUMN)
	binary.Write(w, binary.BigEndian, sqlcol)
}

func (sqlcol *SQLColumn) Decode(r io.Reader) {
	var size uint16
	binary.Read(r, binary.BigEndian, size)
	buff := make([]byte, size)
	r.Read(buff)
	*sqlcol = SQLColumn(buff)
}

// check if sql value type mathches column type
func ValueTypeVsColumnType(valueType SQLType, columnType Types) bool {
	if columnType == CT_INT {
		return valueType == ST_INT
	} else if columnType == CT_FLOAT {
		return valueType == ST_FLOAT
	} else if columnType == CT_STRING {
		return valueType == ST_STRING
	}
	return false
}
