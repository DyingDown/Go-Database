package ast

type SQLStatement interface {
	Type() string
}
