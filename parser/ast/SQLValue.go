package ast

import (
	"bytes"
	"encoding/gob"
	"go-database/parser/token"
	"go-database/util"
	"strconv"
)

type SQLType int

const (
	ST_INT SQLType = iota
	ST_FLOAT
	ST_STRING
	ST_COLUMN
	ST_ILLEGAL
)

type SQLValue struct {
	Value     string
	ValueType SQLType
}

func NewSQLValue(val token.Token) SQLValue {
	if val.Types == token.INT {
		return SQLValue{val.Value, ST_INT}
	} else if val.Types == token.FLOAT {
		return SQLValue{val.Value, ST_FLOAT}
	} else if val.Types == token.STRING {
		return SQLValue{val.Value, ST_STRING}
	} else if val.Types == token.ID {
		return SQLValue{val.Value, ST_COLUMN}
	} else {
		return SQLValue{val.Value, ST_ILLEGAL}
	}
}

func (sqlvalue *SQLValue) GetInt() int64 {
	num, _ := strconv.ParseInt(sqlvalue.Value, 10, 64)
	return num
}

func (sqlvalue *SQLValue) GetFloat() float64 {
	num, _ := strconv.ParseFloat(sqlvalue.Value, 64)
	return num
}

func (sqlvalue *SQLValue) GetString() string {
	if sqlvalue.ValueType == ST_STRING {
		return sqlvalue.Value
	}
	return ""
}

func (sqlvalue *SQLValue) GetType() SQLType {
	return sqlvalue.ValueType
}

func (sqlvalue *SQLValue) Size() uint32 {
	buff := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buff)
	encoder.Encode(sqlvalue)
	return uint32(buff.Len())
}

func (sqlvalue *SQLValue) Row() []byte {
	if sqlvalue.ValueType == ST_INT {
		n := sqlvalue.GetInt()
		return util.Int64ToBytes(n)
	} else if sqlvalue.ValueType == ST_FLOAT {
		n := sqlvalue.GetFloat()
		return util.Float64ToBytes(n)
	} else if sqlvalue.ValueType == ST_STRING {
		return []byte(sqlvalue.Value)[:util.BPlusTreeKeyLen+1]
	}
	return make([]byte, 8)
}

type Row []SQLValue

func (row *Row) GetPrimaryKey() SQLValue {
	return (*row)[0]
}

func (row *Row) SetRowData(indexs []int, values []SQLValue) {
	for _, i := range indexs {
		(*row)[i] = values[i]
	}
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
