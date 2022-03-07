package ast

type SQLCreateTableStatement struct {
	TableName string
	Columns   []*SQLColumnDefine
}
