package ast

import (
	"encoding/binary"
	"fmt"
	"go-database/util"
	"io"
)

type SQLType int8

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
	Decode(r io.Reader) uint64
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

func (sqlint *SQLInt) Decode(r io.Reader) uint64 {
	binary.Read(r, binary.BigEndian, sqlint)
	return uint64(8)
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

func (sqlfloat *SQLFloat) Decode(r io.Reader) uint64 {
	binary.Read(r, binary.BigEndian, sqlfloat)
	return uint64(8)
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

func (sqlstring *SQLString) Decode(r io.Reader) uint64 {
	var size uint16
	binary.Read(r, binary.BigEndian, size)
	buff := make([]byte, size)
	r.Read(buff)
	*sqlstring = SQLString(buff)
	return uint64(2 + size)
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
	binary.Write(w, binary.BigEndian, uint16(len(*sqlcol)))
	binary.Write(w, binary.BigEndian, sqlcol)
}

func (sqlcol *SQLColumn) Decode(r io.Reader) uint64 {
	var size uint16
	binary.Read(r, binary.BigEndian, size)
	buff := make([]byte, size)
	r.Read(buff)
	*sqlcol = SQLColumn(buff)
	return uint64(2 + size)
}

// decode an unknown type sql value
func DecodeValue(r io.Reader) (SQLValue, uint64, error) {
	var tp int8
	binary.Read(r, binary.BigEndian, &tp)
	var size uint64 = 1
	if tp == int8(ST_INT) {
		var v SQLInt
		size += v.Decode(r)
		return &v, size, nil
	} else if tp == int8(ST_FLOAT) {
		var v SQLFloat
		size += v.Decode(r)
		return &v, size, nil
	} else if tp == int8(ST_STRING) {
		var v SQLString
		size += v.Decode(r)
		return &v, size, nil
	} else if tp == int8(ST_COLUMN) {
		var v SQLColumn
		size += v.Decode(r)
		return &v, size, nil
	}
	return nil, 0, fmt.Errorf("unknown type %d", tp)
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
