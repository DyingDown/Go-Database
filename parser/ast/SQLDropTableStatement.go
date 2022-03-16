package ast

type SQLDropTableStatement struct {
	TableNames []string
}

func (sql *SQLDropTableStatement) Type() string {
	return "Drop Table"
}
