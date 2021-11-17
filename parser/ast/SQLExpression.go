package ast

import "Go-Database/parser/token"

type SQLExpression struct {
	Exprs []SQLSingleExpression
	Ops   []token.TokenType
}
