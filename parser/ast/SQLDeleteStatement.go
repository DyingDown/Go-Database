package ast

type SQLDeleteStatement struct {
	TableName string
	Expr      SQLExpression
}
