package ast

type SQLUpdateStatement struct {
	TableName string
	Assigns   []SQLAssignStatement
	Expr      *SQLExpression
}

func (sql SQLUpdateStatement) Type() string {
	return "Update"
}
