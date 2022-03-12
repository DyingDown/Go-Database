package ast

type Row struct {
	pos  uint64
	size uint64
	data []SQLValue
}

func (row *Row) GetPrimaryKey() SQLValue {
	return (*row).data[0]
}

func (row *Row) SetRowData(indexs []int, values []SQLValue) {
	for _, i := range indexs {
		(*row).data[i] = values[i]
	}
}
func (row *Row) Data() []SQLValue {
	return row.data
}
