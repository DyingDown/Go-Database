package parser_test

import (
	"go-database/parser"
	"go-database/parser/ast"
	"testing"
)

func TestParseCreateTable(t *testing.T) {
	sql := "create table student (id int, name string);"
	// parser := parser.NewParser(sql)
	stmt := parser.ParseStatement(sql)
	createStmt := stmt.(*ast.SQLCreateTableStatement)
	if createStmt.TableName != "student" {
		t.Errorf("Expected %v, got %v", "student", createStmt.TableName)
	}
	if len(createStmt.Columns) != 2 {
		t.Errorf("Expected %d, got %d", 2, len(createStmt.Columns))
	}
	if createStmt.Columns[0].ColumnName != "id" {
		t.Errorf("Expected %v, got %v", "id", createStmt.Columns[0].ColumnName)
	}
	if createStmt.Columns[0].ColumnType != ast.CT_INT {
		t.Errorf("Expected %v, got %v", "int", createStmt.Columns[0].ColumnType.String())
	}

	if createStmt.Columns[1].ColumnType != ast.CT_STRING {
		t.Errorf("Expected %v, got %v", "int", createStmt.Columns[0].ColumnType.String())
	}
}

func TestInsert(t *testing.T) {
	sql := "insert into student (id, name) values (1, 'John Doe');"
	stmt := parser.ParseStatement(sql)
	insertStmt := stmt.(*ast.SQLInsertStatement)
	if insertStmt.TableName != "student" {
		t.Errorf("Expected %v, got %v", "student", insertStmt.TableName)
	}
	if len(insertStmt.ColumnNames) != 2 {
		t.Errorf("Expected %d, got %d", 2, len(insertStmt.ColumnNames))
	}
	if insertStmt.ColumnNames[0] != "id" {
		t.Errorf("Expected %v, got %v", "id", insertStmt.ColumnNames[0])
	}
	if insertStmt.ColumnNames[1] != "name" {
		t.Errorf("Expected %v, got %v", "name", insertStmt.ColumnNames[1])
	}
	if len(insertStmt.Values) != 2 {
		t.Errorf("Expected %d, got %d", 2, len(insertStmt.Values))
	}
	if insertStmt.Values[0].GetType() != ast.ST_INT {
		t.Errorf("Expected %v, got %v", "int", insertStmt.Values[0].GetType())
	}
	if insertStmt.Values[1].GetType() != ast.ST_STRING {
		t.Errorf("Expected %v, got %v", "string", insertStmt.Values[1].GetType())
	}
	if *insertStmt.Values[0].(*ast.SQLInt) != 1 {
		t.Errorf("Expected %v", "1")
	}
	if *insertStmt.Values[1].(*ast.SQLString) != "John Doe" {
		t.Errorf("Expected %v", "John Doe")
	}

}
