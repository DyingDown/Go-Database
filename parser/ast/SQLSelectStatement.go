package ast

type SQLSelectStatement struct {
	SelectList []*SQLSelectListElement
	Table      string
	Expr       *SQLExpression
}

func (sql *SQLSelectStatement) Type() string {
	return "Select Statement"
}
