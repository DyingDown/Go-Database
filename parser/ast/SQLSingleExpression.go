package ast

import "go-database/parser/token"

type SQLSingleExpression struct {
	LeftVal   SQLValue
	CompareOp token.TokenType
	RightVal  SQLValue
}

func (sql *SQLSingleExpression) IsEqual() bool {
	return sql.CompareOp == token.EQUAL
}

func (sql *SQLSingleExpression) NotEqual() bool {
	return sql.CompareOp == token.NOT_EQUAL
}

func (sql *SQLSingleExpression) Type() string {
	return "one expression"
}
