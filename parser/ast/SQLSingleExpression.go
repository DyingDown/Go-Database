package ast

import "go-database/parser/token"

type SQLSingleExpression struct {
	LeftVal   SQLValue
	CompareOp token.TokenType
	RightVal  SQLValue
}

func (sse *SQLSingleExpression) IsEqual() bool {
	return sse.CompareOp == token.EQUAL
}

func (sse *SQLSingleExpression) NotEqual() bool {
	return sse.CompareOp == token.NOT_EQUAL
}
