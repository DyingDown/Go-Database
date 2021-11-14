package parser

import (
	"strings"
	"unicode"
)

type Tokenizer struct {
	currentPosition int
	Sql_str         string
	Sql_len         int
	Tokens          []Token
	CurrentTokenPos int
}

func NewTokenizer(content string) Tokenizer {
	tokenizer := Tokenizer{
		0,
		content,
		len(content),
		make([]Token, 0),
		0,
	}
	tokenizer.getAllTokens()
	return tokenizer
}

func (tokenizer *Tokenizer) scanNextToken() (currentToken Token) {
	for tokenizer.currentPosition < tokenizer.Sql_len && tokenizer.isSpace(tokenizer.Sql_str[tokenizer.currentPosition]) {
		tokenizer.currentPosition++
	}
	if tokenizer.currentPosition >= tokenizer.Sql_len {
		currentToken = Token{Types: END, Value: ""}
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

func (tokenizer *Tokenizer) getString() Token {
	quotation := tokenizer.Sql_str[tokenizer.currentPosition]
	var str string
	tokenizer.currentPosition++
	for tokenizer.currentPosition < tokenizer.Sql_len && tokenizer.Sql_str[tokenizer.currentPosition] != quotation {
		str += string(tokenizer.Sql_str[tokenizer.currentPosition])
		tokenizer.currentPosition++
	}
	if tokenizer.currentPosition < tokenizer.Sql_len && tokenizer.Sql_str[tokenizer.currentPosition] == quotation {
		tokenizer.currentPosition++
		return Token{Types: STRING, Value: str}
	}
	return Token{Types: ILLEGAL, Value: str}
}

func (tokenizer *Tokenizer) getNumber() Token {
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
		return Token{Types: INT, Value: number}
	} else if dot == 1 {
		if number == "." {
			return Token{Types: DOT, Value: number}
		}
		return Token{Types: FLOAT, Value: number}
	} else {
		return Token{Types: ILLEGAL, Value: number}
	}
}

func (tokenizer *Tokenizer) getWords() Token {
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
		return Token{Types: ILLEGAL, Value: str}
	}
	str = strings.ToLower(str)
	if str == "add" {
		return Token{Types: ADD, Value: str}
	} else if str == "alter" {
		return Token{Types: ALTER, Value: str}
	} else if str == "all" {
		return Token{Types: ALL, Value: str}
	} else if str == "and" {
		return Token{Types: AND, Value: str}
	} else if str == "any" {
		return Token{Types: ANY, Value: str}
	} else if str == "as" {
		return Token{Types: AS, Value: str}
	} else if str == "asc" {
		return Token{Types: ASC, Value: str}
	} else if str == "avg" {
		return Token{Types: AVG, Value: str}
	} else if str == "by" {
		return Token{Types: BY, Value: str}
	} else if str == "check" {
		return Token{Types: CHECK, Value: str}
	} else if str == "column" {
		return Token{Types: COLUMN, Value: str}
	} else if str == "count" {
		return Token{Types: COUNT, Value: str}
	} else if str == "create" {
		return Token{Types: CREATE, Value: str}
	} else if str == "delete" {
		return Token{Types: DELETE, Value: str}
	} else if str == "desc" {
		return Token{Types: DESC, Value: str}
	} else if str == "drop" {
		return Token{Types: DROP, Value: str}
	} else if str == "distinct" {
		return Token{Types: DISTINCT, Value: str}
	} else if str == "except" {
		return Token{Types: EXCEPT, Value: str}
	} else if str == "foreign" {
		return Token{Types: FOREIGN, Value: str}
	} else if str == "from" {
		return Token{Types: FROM, Value: str}
	} else if str == "group" {
		return Token{Types: GROUP, Value: str}
	} else if str == "having" {
		return Token{Types: HAVING, Value: str}
	} else if str == "in" {
		return Token{Types: IN, Value: str}
	} else if str == "index" {
		return Token{Types: INDEX, Value: str}
	} else if str == "is" {
		return Token{Types: IS, Value: str}
	} else if str == "insert" {
		return Token{Types: INSERT, Value: str}
	} else if str == "into" {
		return Token{Types: INTO, Value: str}
	} else if str == "join" {
		return Token{Types: JOIN, Value: str}
	} else if str == "key" {
		return Token{Types: KEY, Value: str}
	} else if str == "like" {
		return Token{Types: LIKE, Value: str}
	} else if str == "min" {
		return Token{Types: MIN, Value: str}
	} else if str == "max" {
		return Token{Types: MAX, Value: str}
	} else if str == "not" {
		return Token{Types: NOT, Value: str}
	} else if str == "null" {
		return Token{Types: NUL, Value: str}
	} else if str == "||" {
		return Token{Types: OR, Value: str}
	} else if str == "order" {
		return Token{Types: ORDER, Value: str}
	} else if str == "primary" {
		return Token{Types: PRIMARY, Value: str}
	} else if str == "table" {
		return Token{Types: TABLE, Value: str}
	} else if str == "select" {
		return Token{Types: SELECT, Value: str}
	} else if str == "set" {
		return Token{Types: SET, Value: str}
	} else if str == "sum" {
		return Token{Types: SUM, Value: str}
	} else if str == "update" {
		return Token{Types: UPDATE, Value: str}
	} else if str == "union" {
		return Token{Types: UNION, Value: str}
	} else if str == "values" {
		return Token{Types: VALUES, Value: str}
	} else if str == "where" {
		return Token{Types: WHERE, Value: str}
	} else {
		return Token{Types: ID, Value: str}
	}
}

func (tokenizer *Tokenizer) getPunct() Token {
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
		return Token{Types: PLUS, Value: str}
	} else if str == "-" {
		return Token{Types: MINUS, Value: str}
	} else if str == "*" {
		return Token{Types: ASTERISK, Value: str}
	} else if str == "/" {
		return Token{Types: DIVISION, Value: str}
	} else if str == ";" {
		return Token{Types: SEMICOLON, Value: str}
	} else if str == "," {
		return Token{Types: COMMA, Value: str}
	} else if str == ">" {
		return Token{Types: GREATER_THAN, Value: str}
	} else if str == "<" {
		return Token{Types: LESS_THAN, Value: str}
	} else if str == "(" {
		return Token{Types: L_BRACKET, Value: str}
	} else if str == ")" {
		return Token{Types: R_BRACKET, Value: str}
	} else if str == "=" {
		return Token{Types: EQUAL, Value: str}
	} else if str == ">=" {
		return Token{Types: GREATER_EQUAL_TO, Value: str}
	} else if str == "<=" {
		return Token{Types: LESS_EQUAL_TO, Value: str}
	} else if str == "!=" {
		return Token{Types: NOT_EQUAL, Value: str}
	} else if str == "<>" {
		return Token{Types: NOT_EQUAL, Value: str}
	} else if str == "." {
		return Token{Types: DOT, Value: str}
	} else {
		return Token{Types: ILLEGAL, Value: str}
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
		if tokenizer.Tokens[len(tokenizer.Tokens)-1].Types == END {
			break
		}
	}
}

func (tokenizer *Tokenizer) getNextToken() Token {
	if tokenizer.CurrentTokenPos < len(tokenizer.Tokens) {
		tokenizer.CurrentTokenPos++
		return tokenizer.Tokens[tokenizer.CurrentTokenPos-1]
	} else {
		return Token{Types: END, Value: ""}
	}
}

func (tokenizer *Tokenizer) getCurrentToken() Token {
	if tokenizer.CurrentTokenPos < len(tokenizer.Tokens) {
		return tokenizer.Tokens[tokenizer.CurrentTokenPos]
	} else {
		return Token{Types: END, Value: ""}
	}
}

func (tokenizer *Tokenizer) traceBack() {
	tokenizer.CurrentTokenPos--
}
