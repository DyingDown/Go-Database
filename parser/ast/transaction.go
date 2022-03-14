package ast

type BeginTransaction string
type CommitTransaction string
type AbortTransaction string

func (sql BeginTransaction) Type() string {
	return "BEGIN"
}

func (sql CommitTransaction) Type() string {
	return "COMMIT"
}

func (sql AbortTransaction) Type() string {
	return "ABORT"
}
