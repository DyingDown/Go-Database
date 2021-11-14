package ast

import "Go-Database/parser"

type SQLSingleExpression struct {
	LeftVal   SQLValue
	CompareOp parser.TokenType
	RightVal  SQLValue
}
