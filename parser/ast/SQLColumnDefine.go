package ast

type Types int

const (
	CT_INT Types = iota
	CT_FLOAT
	CT_STRING
)

var StringToType = map[string]Types{"int": CT_INT, "float": CT_FLOAT, "string": CT_STRING}

type SQLColumnDefine struct {
	columnName string
	columnType Types
	len        int
}

func NewSQLColumnDefine(columnName string, columnType Types) SQLColumnDefine {
	columnDefine := SQLColumnDefine{
		columnName,
		columnType,
		500,
	}
	return columnDefine
}
