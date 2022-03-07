package ast

type SQLInsertStatement struct {
	TableName   string
	ColumnNames []string
	Values      []SQLValue
}

func (sql *SQLInsertStatement) ValueSize() uint32 {
	var size uint32
	for i := range sql.Values {
		size += sql.Values[i].Size()
	}
	return size
}
