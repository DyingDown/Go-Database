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
func (vm *VersionManager) Select(xid uint64, stmt ast.SQLSelectStatement) ([]*ast.Row, error) {
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
		vis, err := isVisible(transaction, row)
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
func (vm *VersionManager) Insert(xid uint64, stmt ast.SQLInsertStatement) error {
	vm.lock.Lock()
	// check if xid is valid
	transaction, ok := vm.activeTransactions[xid]
	vm.lock.Unlock()
	if !ok {
		panic("xid is invalid")
	}
	stmt.Values = append(stmt.Values, ast.SQLValue{})
	// insert the row
	err := vm.dm.InsertData(stmt)
	if err != nil {
		return err
	}
}

// @description: Check if the certain row is invisible for this transaction
func isVisible(transaction *Transaction, row *ast.Row) (bool, error) {

	return false, nil
}
