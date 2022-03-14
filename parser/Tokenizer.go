package parser

import (
	"go-database/parser/token"
	"strings"
	"unicode"
)

type Tokenizer struct {
	currentPosition int
	Sql_str         string
	Sql_len         int
	Tokens          []token.Token
	CurrentTokenPos int
}

func NewTokenizer(content string) Tokenizer {
	tokenizer := Tokenizer{
		0,
		content,
		len(content),
		make([]token.Token, 0),
		0,
	}
	tokenizer.getAllTokens()
	return tokenizer
}

func (tokenizer *Tokenizer) scanNextToken() (currentToken token.Token) {
	for tokenizer.currentPosition < tokenizer.Sql_len && tokenizer.isSpace(tokenizer.Sql_str[tokenizer.currentPosition]) {
		tokenizer.currentPosition++
	}
	if tokenizer.currentPosition >= tokenizer.Sql_len {
		currentToken = token.Token{Types: token.END, Value: ""}
	} else if tokenizer.Sql_str[tokenizer.currentPosition] == '"' || tokenizer.Sql_str[tokenizer.currentPosition] == '\'' {
		currentToken = tokenizer.getString()
	} else if unicode.IsDigit(rune(tokenizer.Sql_str[tokenizer.currentPosition])) {
		currentToken = tokenizer.getNumber()
	} else if unicode.IsLetter(rune(tokenizer.Sql_str[tokenizer.currentPosition])) {
		currentToken = tokenizer.getWords()
	} else {
		currentToken = tokenizer.getPunct()
	}
	return currentToken
}

func (tokenizer *Tokenizer) getString() token.Token {
	quotation := tokenizer.Sql_str[tokenizer.currentPosition]
	var str string
	tokenizer.currentPosition++
	for tokenizer.currentPosition < tokenizer.Sql_len && tokenizer.Sql_str[tokenizer.currentPosition] != quotation {
		str += string(tokenizer.Sql_str[tokenizer.currentPosition])
		tokenizer.currentPosition++
	}
	if tokenizer.currentPosition < tokenizer.Sql_len && tokenizer.Sql_str[tokenizer.currentPosition] == quotation {
		tokenizer.currentPosition++
		return token.Token{Types: token.STRING, Value: str}
	}
	return token.Token{Types: token.ILLEGAL, Value: str}
}

func (tokenizer *Tokenizer) getNumber() token.Token {
	var number string
	dot := 0
	currentChar := tokenizer.Sql_str[tokenizer.currentPosition]
	for (currentChar == '.' || unicode.IsDigit(rune(currentChar))) && tokenizer.currentPosition < tokenizer.Sql_len {
		if currentChar == '.' {
			if dot == 1 {

			}
			dot++
		}
		number += string(currentChar)
		tokenizer.currentPosition++
		currentChar = tokenizer.Sql_str[tokenizer.currentPosition]
	}
	if dot == 0 {
		return token.Token{Types: token.INT, Value: number}
	} else if dot == 1 {
		if number == "." {
			return token.Token{Types: token.DOT, Value: number}
		}
		return token.Token{Types: token.FLOAT, Value: number}
	} else {
		return token.Token{Types: token.ILLEGAL, Value: number}
	}
}

func (tokenizer *Tokenizer) getWords() token.Token {
	var str string
	illegalCharNum := 0
	for tokenizer.currentPosition < tokenizer.Sql_len &&
		(unicode.IsLetter(rune(tokenizer.Sql_str[tokenizer.currentPosition])) ||
			tokenizer.Sql_str[tokenizer.currentPosition] == '_') {
		if tokenizer.Sql_str[tokenizer.currentPosition] != '_' &&
			!unicode.IsLetter(rune(tokenizer.Sql_str[tokenizer.currentPosition])) &&
			!unicode.IsDigit(rune(tokenizer.Sql_str[tokenizer.currentPosition])) {
			illegalCharNum++
			break
		}
		str += string(tokenizer.Sql_str[tokenizer.currentPosition])
		tokenizer.currentPosition++
	}
	if illegalCharNum != 0 {
		return token.Token{Types: token.ILLEGAL, Value: str}
	}
	str = strings.ToLower(str)
	if str == "add" {
		return token.Token{Types: token.ADD, Value: str}
	} else if str == "abort" {
		return token.Token{Types: token.ABORT, Value: str}
	} else if str == "alter" {
		return token.Token{Types: token.ALTER, Value: str}
	} else if str == "all" {
		return token.Token{Types: token.ALL, Value: str}
	} else if str == "and" {
		return token.Token{Types: token.AND, Value: str}
	} else if str == "any" {
		return token.Token{Types: token.ANY, Value: str}
	} else if str == "as" {
		return token.Token{Types: token.AS, Value: str}
	} else if str == "asc" {
		return token.Token{Types: token.ASC, Value: str}
	} else if str == "avg" {
		return token.Token{Types: token.AVG, Value: str}
	} else if str == "begin" {
		return token.Token{Types: token.BEGIN, Value: str}
	} else if str == "by" {
		return token.Token{Types: token.BY, Value: str}
	} else if str == "check" {
		return token.Token{Types: token.CHECK, Value: str}
	} else if str == "column" {
		return token.Token{Types: token.COLUMN, Value: str}
	} else if str == "commit" {
		return token.Token{Types: token.COMMIT, Value: str}
	} else if str == "count" {
		return token.Token{Types: token.COUNT, Value: str}
	} else if str == "create" {
		return token.Token{Types: token.CREATE, Value: str}
	} else if str == "delete" {
		return token.Token{Types: token.DELETE, Value: str}
	} else if str == "desc" {
		return token.Token{Types: token.DESC, Value: str}
	} else if str == "drop" {
		return token.Token{Types: token.DROP, Value: str}
	} else if str == "distinct" {
		return token.Token{Types: token.DISTINCT, Value: str}
	} else if str == "except" {
		return token.Token{Types: token.EXCEPT, Value: str}
	} else if str == "foreign" {
		return token.Token{Types: token.FOREIGN, Value: str}
	} else if str == "from" {
		return token.Token{Types: token.FROM, Value: str}
	} else if str == "group" {
		return token.Token{Types: token.GROUP, Value: str}
	} else if str == "having" {
		return token.Token{Types: token.HAVING, Value: str}
	} else if str == "in" {
		return token.Token{Types: token.IN, Value: str}
	} else if str == "index" {
		return token.Token{Types: token.INDEX, Value: str}
	} else if str == "is" {
		return token.Token{Types: token.IS, Value: str}
	} else if str == "insert" {
		return token.Token{Types: token.INSERT, Value: str}
	} else if str == "into" {
		return token.Token{Types: token.INTO, Value: str}
	} else if str == "join" {
		return token.Token{Types: token.JOIN, Value: str}
	} else if str == "key" {
		return token.Token{Types: token.KEY, Value: str}
	} else if str == "like" {
		return token.Token{Types: token.LIKE, Value: str}
	} else if str == "min" {
		return token.Token{Types: token.MIN, Value: str}
	} else if str == "max" {
		return token.Token{Types: token.MAX, Value: str}
	} else if str == "not" {
		return token.Token{Types: token.NOT, Value: str}
	} else if str == "null" {
		return token.Token{Types: token.NUL, Value: str}
	} else if str == "||" {
		return token.Token{Types: token.OR, Value: str}
	} else if str == "order" {
		return token.Token{Types: token.ORDER, Value: str}
	} else if str == "primary" {
		return token.Token{Types: token.PRIMARY, Value: str}
	} else if str == "table" {
		return token.Token{Types: token.TABLE, Value: str}
	} else if str == "select" {
		return token.Token{Types: token.SELECT, Value: str}
	} else if str == "set" {
		return token.Token{Types: token.SET, Value: str}
	} else if str == "sum" {
		return token.Token{Types: token.SUM, Value: str}
	} else if str == "update" {
		return token.Token{Types: token.UPDATE, Value: str}
	} else if str == "union" {
		return token.Token{Types: token.UNION, Value: str}
	} else if str == "values" {
		return token.Token{Types: token.VALUES, Value: str}
	} else if str == "where" {
		return token.Token{Types: token.WHERE, Value: str}
	} else {
		return token.Token{Types: token.ID, Value: str}
	}
}

func (tokenizer *Tokenizer) getPunct() token.Token {
	var str string
	for unicode.IsPunct(rune(tokenizer.Sql_str[tokenizer.currentPosition])) && tokenizer.currentPosition < tokenizer.Sql_len {
		str += string(tokenizer.Sql_str[tokenizer.currentPosition])
		tokenizer.currentPosition++
		if str == "+" || str == "-" || str == "*" ||
			str == "/" || str == "," || str == ";" ||
			str == "(" || str == ")" {
			break
		}
	}
	if str == "+" {
		return token.Token{Types: token.PLUS, Value: str}
	} else if str == "-" {
		return token.Token{Types: token.MINUS, Value: str}
	} else if str == "*" {
		return token.Token{Types: token.ASTERISK, Value: str}
	} else if str == "/" {
		return token.Token{Types: token.DIVISION, Value: str}
	} else if str == ";" {
		return token.Token{Types: token.SEMICOLON, Value: str}
	} else if str == "," {
		return token.Token{Types: token.COMMA, Value: str}
	} else if str == ">" {
		return token.Token{Types: token.GREATER_THAN, Value: str}
	} else if str == "<" {
		return token.Token{Types: token.LESS_THAN, Value: str}
	} else if str == "(" {
		return token.Token{Types: token.L_BRACKET, Value: str}
	} else if str == ")" {
		return token.Token{Types: token.R_BRACKET, Value: str}
	} else if str == "=" {
		return token.Token{Types: token.EQUAL, Value: str}
	} else if str == ">=" {
		return token.Token{Types: token.GREATER_EQUAL_TO, Value: str}
	} else if str == "<=" {
		return token.Token{Types: token.LESS_EQUAL_TO, Value: str}
	} else if str == "!=" {
		return token.Token{Types: token.NOT_EQUAL, Value: str}
	} else if str == "<>" {
		return token.Token{Types: token.NOT_EQUAL, Value: str}
	} else if str == "." {
		return token.Token{Types: token.DOT, Value: str}
	} else {
		return token.Token{Types: token.ILLEGAL, Value: str}
	}
}

func (tokenizer *Tokenizer) isSpace(ch byte) bool {
	if ch == '\n' || ch == '\t' || ch == ' ' {
		return true
	}
	return false
}

func (tokenizer *Tokenizer) getAllTokens() {
	for {
		tokenizer.Tokens = append(tokenizer.Tokens, tokenizer.scanNextToken())
		if tokenizer.Tokens[len(tokenizer.Tokens)-1].Types == token.END {
			break
		}
	}
}

func (tokenizer *Tokenizer) getNextToken() token.Token {
	if tokenizer.CurrentTokenPos < len(tokenizer.Tokens) {
		tokenizer.CurrentTokenPos++
		return tokenizer.Tokens[tokenizer.CurrentTokenPos-1]
	} else {
		return token.Token{Types: token.END, Value: ""}
	}
}

func (tokenizer *Tokenizer) getCurrentToken() token.Token {
	if tokenizer.CurrentTokenPos < len(tokenizer.Tokens) {
		return tokenizer.Tokens[tokenizer.CurrentTokenPos]
	} else {
		return token.Token{Types: token.END, Value: ""}
	}
}

func (tokenizer *Tokenizer) traceBack() {
	tokenizer.CurrentTokenPos--
}
