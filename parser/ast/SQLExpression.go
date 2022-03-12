package ast

import "go-database/parser/token"

type SQLExpression struct {
	Exprs []*SQLSingleExpression
	Ops   []token.TokenType
}

func (expr *SQLExpression) IsWhereExists() bool {
	if len(expr.Exprs) == 0 && len(expr.Ops) == 0 {
		return false
	}
	return true
}
