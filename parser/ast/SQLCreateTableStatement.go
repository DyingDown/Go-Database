package ast

type SQLCreateTableStatement struct {
	TableName string
	Columns   []*SQLColumnDefine
}

func (sql SQLCreateTableStatement) Type() string {
	return "Create Table"
}
