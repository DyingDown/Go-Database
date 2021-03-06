package ast

import (
	"go-database/storage/index"
)

type Types int

const (
	CT_INT Types = iota
	CT_FLOAT
	CT_STRING
)

func (types Types) String() string {
	switch types {
	case CT_INT:
		return "int"
	case CT_FLOAT:
		return "float"
	case CT_STRING:
		return "string"
	}
	return ""
}

var StringToType = map[string]Types{"int": CT_INT, "float": CT_FLOAT, "string": CT_STRING}

type SQLColumnDefine struct {
	ColumnName string
	ColumnType Types
	Len        int
	Index      index.Index
	tableId    uint32
}

func NewSQLColumnDefine(columnName string, columnType Types) *SQLColumnDefine {
	return &SQLColumnDefine{
		ColumnName: columnName,
		ColumnType: columnType,
		Len:        500,
	}
}

func (sql *SQLColumnDefine) Type() string {
	return "Column Define"
}
