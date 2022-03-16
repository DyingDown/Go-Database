package token

type TokenType int

const (
	INT TokenType = iota
	STRING
	FLOAT
	ID
	ABORT
	ADD
	ALTER
	ALL
	AND
	ANY
	AS
	ASC // 升序
	AVG
	BY
	BEGIN
	CHECK
	COLUMN
	COMMIT
	COUNT
	CREATE
	DELETE
	DESC // descending  降序
	DROP // keywords
	DISTINCT
	EXCEPT
	FOREIGN
	FROM
	GROUP
	HAVING
	IN
	INDEX
	INSERT
	INTO
	IS
	JOIN
	KEY
	LIKE
	MIN
	MAX
	NOT
	NUL
	OR
	ORDER
	PRIMARY
	TABLE
	SELECT
	SET
	SUM
	UPDATE
	UNION
	VALUES
	WHERE

	L_BRACKET        // (
	R_BRACKET        // )
	SEMICOLON        // ;
	COMMA            // ,
	DOT              // .
	ASTERISK         // '*'
	PLUS             // +
	MINUS            // -
	DIVISION         // /
	GREATER_THAN     // >
	LESS_THAN        // <
	GREATER_EQUAL_TO // >=
	LESS_EQUAL_TO    // <=
	EQUAL            // =
	NOT_EQUAL        // != <>
	ILLEGAL
	END
)

type Token struct {
	Types TokenType
	Value string
}

func (tokenType TokenType) String() string {
	switch tokenType {
	case INT:
		return "INT"
	case STRING:
		return "STRING"
	case FLOAT:
		return "FLOAT"
	case ID:
		return "ID"
	case ABORT:
		return "ABORT"
	case ADD:
		return "ADD"
	case ALTER:
		return "ALTER"
	case ALL:
		return "ALL"
	case AND:
		return "AND"
	case ANY:
		return "ANY"
	case AS:
		return "AS"
	case ASC:
		return "ASC"
	case AVG:
		return "AVG"
	case BY:
		return "BY"
	case BEGIN:
		return "BEGIN"
	case CHECK:
		return "CHECK"
	case COLUMN:
		return "COLUMN"
	case COMMIT:
		return "COMMIT"
	case COUNT:
		return "COUNT"
	case CREATE:
		return "CREATE"
	case DELETE:
		return "DELETE"
	case DESC:
		return "DESC"
	case DROP:
		return "DROP"
	case DISTINCT:
		return "DISTINCT"
	case EXCEPT:
		return "EXCEPT"
	case FOREIGN:
		return "FOREIGN"
	case FROM:
		return "FROM"
	case GROUP:
		return "GROUP"
	case HAVING:
		return "HAVING"
	case IN:
		return "IN"
	case INDEX:
		return "INDEX"
	case INSERT:
		return "INSERT"
	case INTO:
		return "INTO"
	case IS:
		return "IS"
	case JOIN:
		return "JOIN"
	case KEY:
		return "KEY"
	case LIKE:
		return "LIKE"
	case MIN:
		return "MIN"
	case MAX:
		return "MAX"
	case NOT:
		return "NOT"
	case NUL:
		return "NUL"
	case OR:
		return "OR"
	case ORDER:
		return "ORDER"
	case PRIMARY:
		return "PRIMARY"
	case TABLE:
		return "TABLE"
	case SELECT:
		return "SELECT"
	case SET:
		return "SET"
	case SUM:
		return "SUM"
	case UPDATE:
		return "UPDATE"
	case UNION:
		return "UNION"
	case VALUES:
		return "VALUES"
	case WHERE:
		return "WHERE"
	case L_BRACKET:
		return "L_BRACKET"
	case R_BRACKET:
		return "R_BRACKET"
	case SEMICOLON:
		return "SEMICOLON"
	case COMMA:
		return "COMMA"
	case DOT:
		return "DOT"
	case ASTERISK:
		return "ASTERISK"
	case PLUS:
		return "PLUS"
	case MINUS:
		return "MINUS"
	case DIVISION:
		return "DIVISION"
	case GREATER_THAN:
		return "GREATER_THAN"
	case LESS_THAN:
		return "LESS_THAN"
	case GREATER_EQUAL_TO:
		return "GREATER_EQUAL_TO"
	case LESS_EQUAL_TO:
		return "LESS_EQUAL_TO"
	case EQUAL:
		return "EQUAL"
	case NOT_EQUAL:
		return "NOT_EQUAL"
	case ILLEGAL:
		return "ILLEGAL"
	case END:
		return "END"
	default:
		return "UNKNOWN"
	}
}
