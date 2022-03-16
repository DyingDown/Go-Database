package ast

type SQLAssignStatement struct {
	ColumnName string
	Value      SQLValue
}

func (sql *SQLAssignStatement) Type() string {
	return "Assign"
}
