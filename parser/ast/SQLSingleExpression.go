package ast

import "Go-Database/parser/token"

type SQLSingleExpression struct {
	LeftVal   SQLValue
	CompareOp token.TokenType
	RightVal  SQLValue
}
