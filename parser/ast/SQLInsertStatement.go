package ast

type SQLInsertStatement struct {
	TableName   string
	ColumnNames []string
	Values      []SQLValue
}

func (sql *SQLInsertStatement) Type() string {
	return "Insert"
}
