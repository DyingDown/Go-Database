package ast

type SQLSelectListElement struct {
	TableName     string
	ColumnName    string
	NewColumnName string
}

func (sql *SQLSelectListElement) Type() string {
	return "Select"
}
