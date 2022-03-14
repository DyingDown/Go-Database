package ast

type SQLDeleteStatement struct {
	TableName string
	Expr      *SQLExpression
}

func (sql SQLDeleteStatement) Type() string {
	return "Delete Statement"
}
