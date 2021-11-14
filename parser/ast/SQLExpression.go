package ast

import "Go-Database/parser"

type SQLExpression struct {
	Exprs []SQLSingleExpression
	Ops   []parser.TokenType
}
