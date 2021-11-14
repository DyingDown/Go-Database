package ast

type SQLAssignStatement struct {
	ColumnName string
	Value      SQLValue
}
