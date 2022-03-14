package vm

import (
	"go-database/parser/ast"
	"go-database/storage/dm"
	"sync"
)

type VersionManager struct {
	dm                 *dm.DataManager
	tm                 *TransactionManager
	activeTransactions map[uint64]*Transaction
	lock               sync.RWMutex
}

func CreateVM(dm *dm.DataManager, path string) *VersionManager {
	tm := CreateTM(path)
	return &VersionManager{
		dm:                 dm,
		tm:                 tm,
		activeTransactions: make(map[uint64]*Transaction),
	}
}

func OpenVM(dm *dm.DataManager, path string) *VersionManager {
	tm := OpenTM(path)
	return &VersionManager{
		dm:                 dm,
		tm:                 tm,
		activeTransactions: make(map[uint64]*Transaction),
	}
}

// @description: start a transaction
// @return: xid
func (vm *VersionManager) BeginTransaction() uint64 {
	vm.lock.Lock()
	defer vm.lock.Unlock()
	// find the next xid
	xid := vm.tm.Begin()
	newTransaction := NewTransaction(xid, vm.activeTransactions)
	vm.activeTransactions[xid] = newTransaction
	return xid
}

// @description: commit a transaction
func (vm *VersionManager) CommitTransaction(xid uint64) {
	vm.lock.Lock()
	// check if xid is valid
	if _, ok := vm.activeTransactions[xid]; !ok {
		panic("xid is invalid")
	}
	// update the max xid
	vm.tm.MaxXid = xid
	// delete the transaction
	delete(vm.activeTransactions, xid)
	vm.lock.Unlock()
	vm.tm.Commit(xid)
}

// @description: abort a transaction
func (vm *VersionManager) AbortTransaction(xid uint64) {
	vm.lock.Lock()
	// check if xid is valid
	if _, ok := vm.activeTransactions[xid]; !ok {
		panic("xid is invalid")
	}
	// delete the transaction
	delete(vm.activeTransactions, xid)
	vm.lock.Unlock()
	vm.tm.Abort(xid)
}

// @description: Select Rows from tables
func (vm *VersionManager) Select(xid uint64, stmt *ast.SQLSelectStatement) ([]*ast.Row, error) {
	vm.lock.Lock()
	// check if xid is valid
	transaction, ok := vm.activeTransactions[xid]
	vm.lock.Unlock()
	if !ok {
		panic("xid is invalid")
	}
	// get rows as channel
	resultChan, err := vm.dm.SelectData(stmt)
	if err != nil {
		return nil, err
	}
	// change channel to slice
	var result []*ast.Row
	for row := range resultChan {
		vis, err := isVisible(transaction, row, vm.tm)
		if err != nil {
			return nil, err
		}
		if vis {
			result = append(result, row)
		}
	}
	return result, nil
}

// @description: Insert a row into table
func (vm *VersionManager) Insert(xid uint64, stmt *ast.SQLInsertStatement) (*ast.Row, error) {
	vm.lock.Lock()
	// check if xid is valid
	_, ok := vm.activeTransactions[xid]
	vm.lock.Unlock()
	if !ok {
		panic("xid is invalid")
	}
	// set start xid and end xid
	xmin := ast.SQLInt(xid)
	stmt.Values = append(stmt.Values, &xmin)
	xmax := ast.SQLInt(NULL_Xid)
	stmt.Values = append(stmt.Values, &xmax)
	// insert the row
	row, err := vm.dm.InsertData(stmt)
	if err != nil {
		return nil, err
	}
	return row, nil
}

// @description: Delete row
func (vm *VersionManager) Delete(xid uint64, stmt *ast.SQLDeleteStatement) ([]*ast.Row, error) {
	vm.lock.Lock()
	// check if xid is valid
	_, ok := vm.activeTransactions[xid]
	vm.lock.Unlock()
	if !ok {
		panic("xid is invalid")
	}
	seletStmt := &ast.SQLSelectStatement{
		Table:      stmt.TableName,
		Expr:       stmt.Expr,
		SelectList: []ast.SQLSelectListElement{{ColumnName: "*"}},
	}
	rows, err := vm.Select(xid, seletStmt)
	if err != nil {
		return nil, err
	}
	rowSlice := make([]*ast.Row, 0)
	// set end xid and turns channel to slice
	for _, row := range rows {
		row.SetMaxXid(xid)
		vm.dm.GetFile().WriteAt(row.Encode(), int64(row.GetPos()))
		rowSlice = append(rowSlice, row)
	}
	return rowSlice, nil
}

// @description: Check if the certain row is invisible for this transaction
func isVisible(transaction *Transaction, row *ast.Row, tm *TransactionManager) (bool, error) {
	xmin := row.MinXid()
	xmax := row.MaxXid()
	xid := transaction.Xid

	// if is created by this transaction and is not deleted, visible
	if xmin == xid && xmax == NULL_Xid {
		return true, nil
	}
	// if is created after this transaction, not visible
	if xmin > xid {
		return false, nil
	}
	// if the transaction is not committed and created before this transaction, not visible
	if !tm.CheckCommited(xmin) {
		return false, nil
	}
	// if transaction is not finished when starting this transaction, not visible
	if _, ok := transaction.SnapShot[xmin]; ok {
		return false, nil
	}
	// if is not deleted, visible
	if xmax == NULL_Xid {
		return true, nil
	}
	// if data is deleted by current transaction, not visible
	if xmax == xid {
		return false, nil
	}
	// if data is deleted by other transaction but not committed, visible
	if !tm.CheckCommited(xmax) {
		return true, nil
	}
	// if data is deleted by other transaction and commit time is later than current transaction, visible
	if xmax > xid {
		return true, nil
	}
	// if data is deleted by other transaction but not commit when starting this transaction
	if _, ok := transaction.SnapShot[xmax]; ok {
		return true, nil
	}
	return false, nil
}
