package ast

import "go-database/parser/token"

type SQLExpression struct {
	Exprs []SQLSingleExpression
	Ops   []token.TokenType
}
