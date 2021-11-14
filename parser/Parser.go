package parser

import (
	"Go-Database/parser/ast"
)

type Parser struct {
	Lex Tokenizer
}

func newParser(sql_stmt string) Parser {
	parser := Parser{NewTokenizer(sql_stmt)}
	return parser
}

func (parser *Parser) Match(targetType TokenType) bool {
	if parser.Lex.getNextToken().Types == targetType {
		return true
	} else {
		parser.Lex.traceBack()
	}
	return false
}

func (parser *Parser) CreateTable() ast.SQLCreateTableStatement {
	stmt := ast.SQLCreateTableStatement{}
	if parser.Match(CREATE) && parser.Match(TABLE) {
		if parser.Lex.getCurrentToken().Types != ID {
			panic("Doesn't declare the table name")
		}
		stmt.TableName = parser.Lex.getNextToken().Value
		if parser.Match(L_BRACKET) {
			matchComma := true
			for matchComma || parser.Match(COMMA) {
				matchComma = false
				stmt.Columns = append(stmt.Columns, parser.getColumnDefine())
			}
			if parser.Match(R_BRACKET) && parser.Match(SEMICOLON) {
				return stmt
			} else {
				panic("No matching ')' or no ending ';'")
			}
		} else {
			return stmt
		}
	} else {
		panic("Not a Create Table Statement!")
	}
}

func (parser *Parser) DeleteRow() ast.SQLDeleteStatement {
	if parser.Match(DELETE) && parser.Match(FROM) {
		if parser.Lex.getCurrentToken().Types != ID {
			panic("Doesn't declare the table name")
		}
		tableName := parser.Lex.getNextToken().Value
		if parser.Match(WHERE) {
			expr := parser.getExpressions()
			if parser.Match(SEMICOLON) {
				return ast.SQLDeleteStatement{tableName, expr}
			} else {
				panic("Missing ';'")
			}
		} else {
			panic("Not a complete delete statement")
		}
	} else {
		panic("Not a Delete Statement")
	}
}

func (parser *Parser) DropTable() []string {
	tablenames := make([]string, 0)
	if parser.Match(DROP) && parser.Match(TABLE) {
		_tk := true
		for _tk || parser.Match(COMMA) {
			_tk = false
			if parser.Lex.getCurrentToken().Types == ID {
				tablenames = append(tablenames, parser.Lex.getNextToken().Value)
			} else {
				panic("Missing table name")
			}
		}
		if parser.Match(SEMICOLON) {
			return tablenames
		} else {
			panic("Missing ';'")
		}
	} else {
		panic("Not a Drop Table Statement")
	}
}

func (parser *Parser) InsertRow() ast.SQLInsertStatement {
	stmt := ast.SQLInsertStatement{}
	if parser.Match(INSERT) && parser.Match(INTO) {
		if parser.Lex.getCurrentToken().Types == ID {
			stmt.TableName = parser.Lex.getNextToken().Value
			if parser.Match(L_BRACKET) {
				_tk := true
				for _tk || parser.Match(COMMA) {
					_tk = false
					if parser.Lex.getCurrentToken().Types == ID {
						stmt.ColumnNames = append(stmt.ColumnNames, parser.Lex.getNextToken().Value)
					} else {
						panic("expeted a column name")
					}
				}
				if !parser.Match(R_BRACKET) {
					panic("missing ')'")
				}
			}
			if !parser.Match(VALUES) {
				panic("missing 'values'")
			}
			if !parser.Match(L_BRACKET) {
				panic("missing '('")
			}
			_tk := true
			for _tk || parser.Match(COMMA) {
				stmt.Values = append(stmt.Values, parser.getValue())
			}
			if !parser.Match(R_BRACKET) {
				panic("missing ')'")
			}
			if !parser.Match(SEMICOLON) {
				panic("missing ';'")
			}
			return stmt
		} else {
			panic("Doesn't specify the table")
		}
	} else {
		panic("Not a insert statement")
	}
}

func (parser *Parser) Select() {
	stmt := ast.SQLSelectStatement{}
	if parser.Match(SELECT) {
		_tk := true
		for _tk || parser.Match(COMMA) {
			_tk = false
			stmt.SelectList = append(stmt.SelectList, parser.getSelectElement())
		}
		if !parser.Match(FROM) {
			panic("Not a valid select, missing word 'from'")
		}
		_tk = true
		for _tk || parser.Match(COMMA) {
			_tk = false
			if parser.Lex.getCurrentToken().Types == ID {
				stmt.TableLists = append(stmt.TableLists, parser.Lex.getNextToken().Value)
			}
		}
		if parser.Lex.getNextToken().Types == WHERE {
			stmt.Expr = parser.getExpressions()
		}
	} else {
		panic("Not a select statement")
	}
}

func (parser *Parser) Update() ast.SQLUpdateStatement {
	stmt := ast.SQLUpdateStatement{}
	if parser.Match(UPDATE) {
		if parser.Lex.getCurrentToken().Types == ID {
			stmt.TableName = parser.Lex.getNextToken().Value
		}
		if !parser.Match(SET) {
			panic("not a valid update, missing word 'set'")
		}
		_tk := true
		for _tk || parser.Match(COMMA) {
			_tk = false
			stmt.Assigns = append(stmt.Assigns, parser.getAssigns())
		}
		if parser.Match(WHERE) {
			stmt.Expr = parser.getExpressions()
		}
	}
	return stmt
}

func (parser *Parser) getColumnDefine() ast.SQLColumnDefine {
	if parser.Lex.getCurrentToken().Types != ID {
		panic("No column name")
	}
	columnName := parser.Lex.getNextToken().Value
	nextToken := parser.Lex.getNextToken()
	if nextToken.Value == "int" || nextToken.Value == "float" || nextToken.Value == "string" {
		return ast.NewSQLColumnDefine(columnName, ast.StringToType[nextToken.Value])
	} else {
		panic("Missing column Type")
	}
}

func (parser *Parser) getExpressions() ast.SQLExpression {
	expression := ast.SQLExpression{}
	exp := parser.getSingleExpression()
	expression.Exprs = append(expression.Exprs, exp)
	for {
		type_ := parser.getLogicOp()
		if type_ == ILLEGAL {
			break
		}
		expression.Ops = append(expression.Ops, type_)
		exp = parser.getSingleExpression()
		expression.Exprs = append(expression.Exprs, exp)
	}
	return expression
}

func (parser *Parser) getSingleExpression() ast.SQLSingleExpression {
	expr := ast.SQLSingleExpression{}
	expr.LeftVal = parser.getValue()
	expr.CompareOp = parser.getCompare()
	expr.RightVal = parser.getValue()
	return expr
}

func (parser *Parser) getValue() ast.SQLValue {
	currentToken := parser.Lex.getNextToken()
	if currentToken.Types == ID ||
		currentToken.Types == STRING ||
		currentToken.Types == INT ||
		currentToken.Types == FLOAT {
		return ast.NewSQLValue(currentToken)
	} else {
		panic("Not a valid parameter")
	}
}

func (parser *Parser) getCompare() TokenType {
	token := parser.Lex.getNextToken()
	if token.Types >= ASTERISK && token.Types <= NOT_EQUAL {
		return token.Types
	} else {
		panic("Not a valid comparison operator")
	}
}

func (parser *Parser) getLogicOp() TokenType {
	token := parser.Lex.getNextToken()
	if token.Types == AND || token.Types == OR {
		return token.Types
	} else {
		parser.Lex.traceBack()
		panic("Not a valid Logic operator")
	}
}

func (parser *Parser) getSelectElement() ast.SQLSelectListElement {
	ele := ast.SQLSelectListElement{}
	token := parser.Lex.getNextToken()
	if token.Types == ASTERISK {
		ele.ColumnName = "*"
	} else if token.Types == ID {
		dot := parser.Lex.getNextToken()
		if dot.Types == DOT {
			t := parser.Lex.getNextToken()
			if t.Types == ID {
				ele.TableName = token.Value
				ele.ColumnNames = t.Value
			} else {
				panic("missing column name")
			}
		} else {
			parser.Lex.traceBack()
			ele.ColumnName = token.Value
		}
		if parser.Match(AS) {
			if parser.Match(ID) {
				ele.NewColumnName = parser.Lex.getCurrentToken().Value
			} else {
				panic("missing new column name")
			}
		}
	} else {
		panic("expected a column name or '*'")
	}
	return ele
}

func (parser *Parser) getAssigns() ast.SQLAssignStatement {
	stmt := ast.SQLAssignStatement{}
	if parser.Lex.getCurrentToken().Types == ID {
		stmt.ColumnName = parser.Lex.getNextToken().Value
		if !parser.Match(EQUAL) {
			panic("not a valid assign statement")
		}
		stmt.Value = parser.getValue()
	}
	return stmt
}
