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

var StringToType = map[string]Types{"int": CT_INT, "float": CT_FLOAT, "string": CT_STRING}

type SQLColumnDefine struct {
	ColumnName string
	ColumnType Types
	len        int
	Index      index.Index
	tableId    uint32
}

func NewSQLColumnDefine(columnName string, columnType Types) *SQLColumnDefine {
	return &SQLColumnDefine{
		ColumnName: columnName,
		ColumnType: columnType,
		len:        500,
	}
}
