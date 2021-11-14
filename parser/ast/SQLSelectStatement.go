package ast

type SQLSelectStatement struct {
	SelectList []SQLSelectListElement
	TableLists []string
	Expr       SQLExpression
}
