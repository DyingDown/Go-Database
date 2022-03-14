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

func (result *Result) Print() {
	for _, i := range result.ColumnName {
		fmt.Println(i, '\t')
	}
	for _, row := range result.Rows {
		line := fmt.Sprintf("%v\n", row)
		fmt.Print(line)
	}
}
