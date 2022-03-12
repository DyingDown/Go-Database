package ast

type SQLSelectStatement struct {
	SelectList []SQLSelectListElement
	Table      string
	Expr       *SQLExpression
}
