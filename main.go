package main

import (
	"fmt"
	"go-database/parser"
)

func main() {
	sql := "Create table color(red int, green string, blue float);"
	// sqlParser := parser.NewTokenizer(sql)
	testCreateTable(sql)
	// fmt.Println(sqlParser.Tokens)
}

func testCreateTable(sql string) {
	parser := parser.NewParser(sql)
	stmt := parser.CreateTable()
	fmt.Println("Table name:", stmt.TableName)
	for i := 0; i < len(stmt.Columns); i++ {
		fmt.Println(stmt.Columns[i].ColumnName, stmt.Columns[i].ColumnType)
	}
}

// func testDeleteStatement(sql string) {
// 	parser := parser.NewParser(sql)

// }
