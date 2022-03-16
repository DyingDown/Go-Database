package tbm

import (
	"fmt"
	"go-database/parser/ast"
)

type Result struct {
	ColumnName []string
	Rows       []*ast.Row
}

func (tbm *TableManager) NewResult(tableName string, rows []*ast.Row) *Result {
	tableInfo := tbm.pager.GetMetaData().GetTableInfo(tableName)
	columnNames := make([]string, 0)
	columns := tableInfo.GetColumns()
	for i := range columns {
		columnNames = append(columnNames, columns[i].ColumnName)
	}
	return &Result{
		ColumnName: columnNames,
		Rows:       rows,
	}
}

func (result *Result) String() string {
	var str string
	for _, i := range result.ColumnName {
		str += fmt.Sprintf("%v\t", i)
	}
	str += "\n"
	for _, row := range result.Rows {
		str += fmt.Sprintf("%v\n", row)
	}
	return str
}
