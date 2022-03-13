package parser

import (
	"go-database/parser/ast"
	"go-database/parser/token"
	"strconv"
)

type Parser struct {
	Lex Tokenizer
}

func NewParser(sql_stmt string) Parser {
	parser := Parser{NewTokenizer(sql_stmt)}
	return parser
}

func (parser *Parser) Match(targetType token.TokenType) bool {
	if parser.Lex.getNextToken().Types == targetType {
		return true
	} else {
		parser.Lex.traceBack()
	}
	return false
}

func (parser *Parser) CreateTable() ast.SQLCreateTableStatement {
	stmt := ast.SQLCreateTableStatement{}
	if parser.Match(token.CREATE) && parser.Match(token.TABLE) {
		if parser.Lex.getCurrentToken().Types != token.ID {
			panic("Doesn't declare the table name")
		}
		stmt.TableName = parser.Lex.getNextToken().Value
		if parser.Match(token.L_BRACKET) {
			matchComma := true
			for matchComma || parser.Match(token.COMMA) {
				matchComma = false
				stmt.Columns = append(stmt.Columns, parser.getColumnDefine())
			}
			if parser.Match(token.R_BRACKET) && parser.Match(token.SEMICOLON) {
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

func (parser *Parser) DeleteRow() *ast.SQLDeleteStatement {
	if parser.Match(token.DELETE) && parser.Match(token.FROM) {
		if parser.Lex.getCurrentToken().Types != token.ID {
			panic("Doesn't declare the table name")
		}
		tableName := parser.Lex.getNextToken().Value
		if parser.Match(token.WHERE) {
			expr := parser.getExpressions()
			if parser.Match(token.SEMICOLON) {
				sdstmt := new(ast.SQLDeleteStatement)
				sdstmt.TableName = tableName
				sdstmt.Expr = expr
				return sdstmt
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
	if parser.Match(token.DROP) && parser.Match(token.TABLE) {
		_tk := true
		for _tk || parser.Match(token.COMMA) {
			_tk = false
			if parser.Lex.getCurrentToken().Types == token.ID {
				tablenames = append(tablenames, parser.Lex.getNextToken().Value)
			} else {
				panic("Missing table name")
			}
		}
		if parser.Match(token.SEMICOLON) {
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
	if parser.Match(token.INSERT) && parser.Match(token.INTO) {
		if parser.Lex.getCurrentToken().Types == token.ID {
			stmt.TableName = parser.Lex.getNextToken().Value
			if parser.Match(token.L_BRACKET) {
				_tk := true
				for _tk || parser.Match(token.COMMA) {
					_tk = false
					if parser.Lex.getCurrentToken().Types == token.ID {
						stmt.ColumnNames = append(stmt.ColumnNames, parser.Lex.getNextToken().Value)
					} else {
						panic("expeted a column name")
					}
				}
				if !parser.Match(token.R_BRACKET) {
					panic("missing ')'")
				}
			}
			if !parser.Match(token.VALUES) {
				panic("missing 'values'")
			}
			if !parser.Match(token.L_BRACKET) {
				panic("missing '('")
			}
			_tk := true
			for _tk || parser.Match(token.COMMA) {
				stmt.Values = append(stmt.Values, parser.getValue())
			}
			if !parser.Match(token.R_BRACKET) {
				panic("missing ')'")
			}
			if !parser.Match(token.SEMICOLON) {
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
	if parser.Match(token.SELECT) {
		_tk := true
		for _tk || parser.Match(token.COMMA) {
			_tk = false
			listEle := parser.getSelectElement()
			stmt.SelectList = append(stmt.SelectList, listEle)
			if listEle.ColumnName == "*" {
				break
			}
		}
		if !parser.Match(token.FROM) {
			panic("Not a valid select, missing word 'from'")
		}
		if parser.Lex.getCurrentToken().Types == token.ID {
			stmt.Table = parser.Lex.getNextToken().Value
		}
		if parser.Lex.getNextToken().Types == token.WHERE {
			stmt.Expr = parser.getExpressions()
		}
	} else {
		panic("Not a select statement")
	}
}

func (parser *Parser) Update() ast.SQLUpdateStatement {
	stmt := ast.SQLUpdateStatement{}
	if parser.Match(token.UPDATE) {
		if parser.Lex.getCurrentToken().Types == token.ID {
			stmt.TableName = parser.Lex.getNextToken().Value
		}
		if !parser.Match(token.SET) {
			panic("not a valid update, missing word 'set'")
		}
		_tk := true
		for _tk || parser.Match(token.COMMA) {
			_tk = false
			stmt.Assigns = append(stmt.Assigns, parser.getAssigns())
		}
		if parser.Match(token.WHERE) {
			stmt.Expr = parser.getExpressions()
		}
	}
	return stmt
}

func (parser *Parser) getColumnDefine() *ast.SQLColumnDefine {
	if parser.Lex.getCurrentToken().Types != token.ID {
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

func (parser *Parser) getExpressions() *ast.SQLExpression {
	expression := new(ast.SQLExpression)
	exp := parser.getSingleExpression()
	expression.Exprs = append(expression.Exprs, exp)
	for {
		type_ := parser.getLogicOp()
		if type_ == token.ILLEGAL {
			break
		}
		expression.Ops = append(expression.Ops, type_)
		exp = parser.getSingleExpression()
		expression.Exprs = append(expression.Exprs, exp)
	}
	return expression
}

func (parser *Parser) getSingleExpression() *ast.SQLSingleExpression {
	expr := new(ast.SQLSingleExpression)
	expr.LeftVal = parser.getValue()
	expr.CompareOp = parser.getCompare()
	expr.RightVal = parser.getValue()
	return expr
}

func (parser *Parser) getValue() ast.SQLValue {
	currentToken := parser.Lex.getNextToken()
	if currentToken.Types == token.ID {
		col := ast.SQLColumn(currentToken.Value)
		return &col
	} else if currentToken.Types == token.STRING {
		str := ast.SQLString(currentToken.Value)
		return &str
	} else if currentToken.Types == token.INT {
		num, err := strconv.ParseInt(currentToken.Value, 10, 0)
		if err != nil {
			panic(err)
		}
		i := ast.SQLInt(num)
		return &i
	} else if currentToken.Types == token.FLOAT {
		num, err := strconv.ParseFloat(currentToken.Value, 64)
		if err != nil {
			panic(err)
		}
		f := ast.SQLFloat(num)
		return &f
	} else {
		panic("Not a valid parameter")
	}
}

func (parser *Parser) getCompare() token.TokenType {
	tk := parser.Lex.getNextToken()
	if tk.Types >= token.ASTERISK && tk.Types <= token.NOT_EQUAL {
		return tk.Types
	} else {
		panic("Not a valid comparison operator")
	}
}

func (parser *Parser) getLogicOp() token.TokenType {
	tk := parser.Lex.getNextToken()
	if tk.Types == token.AND || tk.Types == token.OR {
		return tk.Types
	} else {
		parser.Lex.traceBack()
		panic("Not a valid Logic operator")
	}
}

func (parser *Parser) getSelectElement() ast.SQLSelectListElement {
	ele := ast.SQLSelectListElement{}
	tk := parser.Lex.getNextToken()
	if tk.Types == token.ASTERISK {
		ele.ColumnName = "*"
	} else if tk.Types == token.ID {
		dot := parser.Lex.getNextToken()
		if dot.Types == token.DOT {
			t := parser.Lex.getNextToken()
			if t.Types == token.ID {
				ele.TableName = tk.Value
				ele.ColumnName = t.Value
			} else {
				panic("missing column name")
			}
		} else {
			parser.Lex.traceBack()
			ele.ColumnName = tk.Value
		}
		if parser.Match(token.AS) {
			if parser.Match(token.ID) {
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
	if parser.Lex.getCurrentToken().Types == token.ID {
		stmt.ColumnName = parser.Lex.getNextToken().Value
		if !parser.Match(token.EQUAL) {
			panic("not a valid assign statement")
		}
		stmt.Value = parser.getValue()
	}
	return stmt
}
