package ast

type SQLInsertStatement struct {
	TableName   string
	ColumnNames []string
	Values      []SQLValue
}
