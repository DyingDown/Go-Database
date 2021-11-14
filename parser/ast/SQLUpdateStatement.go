package ast

type SQLUpdateStatement struct {
	TableName string
	Assigns   []SQLAssignStatement
	Expr      SQLExpression
}
