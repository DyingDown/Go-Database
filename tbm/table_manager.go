/*
 * Table Manager: manage all other functions
 */
package tbm

import (
	"fmt"
	"go-database/parser/ast"
	"go-database/storage/bplustree"
	"go-database/storage/dm"
	"go-database/storage/pager"
	"go-database/storage/pager/pagedata"
	"go-database/vm"
)

type TableManager struct {
	pager    *pager.Pager
	dm       *dm.DataManager
	vm       *vm.VersionManager
	tm       *vm.TransactionManager
	metaData *pagedata.MetaData
}

func Open(path string) *TableManager {
	pgr := pager.CreateFile(path)
	DM := dm.CreateDM(path, pgr)
	TM := vm.CreateTM(path)
	VM := vm.CreateVM(DM, TM)
	return &TableManager{
		dm:       DM,
		tm:       TM,
		vm:       VM,
		pager:    pgr,
		metaData: pgr.GetMetaData(),
	}
}

func (tbm *TableManager) Close() {
	tbm.pager.Close()
	tbm.tm.Close()
}

func Create(path string) *TableManager {
	pgr := pager.CreateFile(path)
	DM := dm.CreateDM(path, pgr)
	TM := vm.CreateTM(path)
	VM := vm.CreateVM(DM, TM)
	return &TableManager{
		dm:       DM,
		tm:       TM,
		vm:       VM,
		pager:    pgr,
		metaData: pgr.GetMetaData(),
	}
}

// @description: select data
func (tbm *TableManager) Select(xid uint64, stmt *ast.SQLSelectStatement) (*Result, error) {
	row, err := tbm.vm.Select(xid, stmt)
	if err != nil {
		return nil, err
	}
	return tbm.NewResult(stmt.Table, row), nil
}

// @description: insert row
func (tbm *TableManager) Insert(xid uint64, stmt *ast.SQLInsertStatement) (*Result, error) {
	row, err := tbm.vm.Insert(xid, stmt)
	if err != nil {
		return nil, err
	}
	return tbm.NewResult(stmt.TableName, []*ast.Row{row}), nil
}

// @description: delete row
func (tbm *TableManager) Delete(xid uint64, stmt *ast.SQLDeleteStatement) (*Result, error) {
	rows, err := tbm.vm.Delete(xid, stmt)
	if err != nil {
		return nil, err
	}
	return tbm.NewResult(stmt.TableName, rows), nil
}

// @description: update row
// fisr delet and then insert the updated rows
func (tbm *TableManager) Update(xid uint64, stmt *ast.SQLUpdateStatement) (*Result, error) {
	// create delete statement
	deletStmt := &ast.SQLDeleteStatement{
		TableName: stmt.TableName,
		Expr:      stmt.Expr,
	}
	rows, err := tbm.vm.Delete(xid, deletStmt)
	if err != nil {
		return nil, err
	}
	// if 0 row is deleted, then no rows needs update
	if len(rows) == 0 {
		return nil, nil
	}
	// find column id in table
	ids := make([]int, 0)
	// get newValeus
	newValues := make([]ast.SQLValue, 0)
	tableInfo := tbm.metaData.GetTableInfo(stmt.TableName)
	for _, assign := range stmt.Assigns {
		id, _ := tableInfo.GetColumnInfo(assign.ColumnName)
		ids = append(ids, id)
		newValues = append(newValues, assign.Value)
	}
	resultRows := make([]*ast.Row, 0)
	// update rows
	for _, row := range rows {
		row.SetRowData(ids, newValues)
		copyRow := row.DeepCopy()
		// insert the updated row
		insertStmt := &ast.SQLInsertStatement{
			TableName:   stmt.TableName,
			ColumnNames: tableInfo.GetColumnNames(),
			Values:      copyRow,
		}
		irow, err := tbm.vm.Insert(xid, insertStmt)
		if err != nil {
			return nil, err
		}
		resultRows = append(resultRows, irow)
	}
	return tbm.NewResult(stmt.TableName, resultRows), nil
}

func (tbm *TableManager) CreateTable(xid uint64, stmt *ast.SQLCreateTableStatement) error {
	// check if table name already exists
	if tbm.metaData.GetTableInfo(stmt.TableName) != nil {
		return fmt.Errorf("table %s already exists", stmt.TableName)
	}
	// create table
	newTable := tbm.metaData.NewTableInfo(stmt.TableName, stmt.Columns)

	// set primary key
	newTable.Columns[0].Index = bplustree.NewBPlusTree(tbm.pager, 8, 4, newTable.TableId, 0)
	// create a new record page
	newPage := tbm.pager.CreatePage(pagedata.NewRecordData())
	newTable.FirstPage = newPage.PageNo
	newTable.LastPage = newPage.PageNo
	// add table to meta data
	tbm.metaData.AddTableInfo(newTable)
	return nil
}

// start a transaction
func (tbm *TableManager) Begin() uint64 {
	return tbm.vm.BeginTransaction()
}

// commit a transaction
func (tbm *TableManager) Commit(xid uint64) {
	tbm.vm.CommitTransaction(xid)
}

// rollback a transaction
func (tbm *TableManager) Abort(xid uint64) {
	tbm.vm.AbortTransaction(xid)
}
