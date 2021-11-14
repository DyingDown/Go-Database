package ast

import (
	"Go-Database/parser"
	"strconv"
)

type SQLType int

const (
	INT SQLType = iota
	FLOAT
	STRING
	COLUMN
	ILLEGAL
)

type SQLValue struct {
	Value     string
	ValueType SQLType
}

func NewSQLValue(val parser.Token) SQLValue {
	if val.Types == parser.INT {
		return SQLValue{val.Value, INT}
	} else if val.Types == parser.FLOAT {
		return SQLValue{val.Value, FLOAT}
	} else if val.Types == parser.STRING {
		return SQLValue{val.Value, STRING}
	} else if val.Types == parser.ID {
		return SQLValue{val.Value, COLUMN}
	} else {
		return SQLValue{val.Value, ILLEGAL}
	}
}

func (sqlvalue *SQLValue) getInt() int {
	num, _ := strconv.Atoi(sqlvalue.Value)
	return num
}

func (sqlvalue *SQLValue) getFloat() float64 {
	num, _ := strconv.ParseFloat(sqlvalue.Value, 32)
	return num
}

func (sqlvalue *SQLValue) getString() string {
	return sqlvalue.Value
}
