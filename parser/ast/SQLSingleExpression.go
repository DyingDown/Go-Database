package ast

import "go-database/parser/token"

type SQLSingleExpression struct {
	LeftVal   SQLValue
	CompareOp token.TokenType
	RightVal  SQLValue
}
