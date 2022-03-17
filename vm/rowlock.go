package vm

import (
	"sync"
)

type RowLock struct {
	// 事务持有的数据行
	xidToRow map[uint64]map[int64]bool
	// 持有该数据行的事务
	rowToXid map[int64]uint64
	// 等待该数据行的事务
	rowToWait map[int64]map[uint64]bool
	// 事务等待的数据行
	xidToWait map[uint64]int64
	// 事务的等待channel
	xidToWaitChan map[uint64]chan bool
	lock          sync.Mutex
}

func NewRowLock() *RowLock {
	return &RowLock{
		xidToRow:      make(map[uint64]map[int64]bool),
		rowToXid:      make(map[int64]uint64),
		rowToWait:     make(map[int64]map[uint64]bool),
		xidToWait:     make(map[uint64]int64),
		xidToWaitChan: make(map[uint64]chan bool),
	}
}

func (r *RowLock) init(xid uint64, row int64) {
	// if the xid is not added
	if _, ok := r.xidToRow[xid]; !ok {
		r.xidToRow[xid] = make(map[int64]bool)
	}
	// if the row is not added
	if _, ok := r.rowToWait[row]; !ok {
		r.rowToWait[row] = make(map[uint64]bool)
	}
}

// @description: add a row to the transaction
// 给事务持有row中添加一条row
func (r *RowLock) Add(xid uint64, row int64) (bool, chan bool) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// init the map
	r.init(xid, row)

	// if the row already exists
	if _, ok := r.xidToRow[xid][row]; ok {
		return true, nil
	}

	// if the row is not locked
	if _, ok := r.rowToXid[row]; !ok {
		// 让该事务持有该数据行
		r.rowToXid[row] = xid
		// 事务持有的数据行中加入这一行
		r.xidToRow[xid][row] = true
		return true, nil
	}

	// if the row is locked, add wait relations
	// 让该数据行被该事务等待
	r.rowToWait[row][xid] = true
	// 让该事务等待该数据行
	r.xidToWait[xid] = row
	if r.hasDeadLock(xid) {
		// 如果有死锁，则返回false,并撤销添加的等待
		delete(r.rowToWait[row], xid)
		delete(r.xidToWait, xid)
		return false, nil
	}

	// if no dead locks, add wait channel
	ch := make(chan bool)
	r.xidToWaitChan[xid] = ch
	return true, ch
}

// @description: check is there is dead lock in transaction and row relationship
// RowA is being hold by TransactionA
// TransactionA waits RowB
// RowB is being hold by TransactionB
// TransactionB waits RowA
// @param: visited: the transaction is been visited or not
var vis map[uint64]bool

func (rowlock *RowLock) hasDeadLock(xid uint64) bool {
	vis = make(map[uint64]bool)
	return rowlock.dfs(xid)
}

func (rowlock *RowLock) dfs(xid uint64) bool {
	if _, ok := vis[xid]; ok {
		return false
	}
	vis[xid] = true
	// 事务等待的row
	waitedRow, ok := rowlock.xidToWait[xid]
	// if transaction waits no row, no circle
	if !ok {
		return false
	}
	// row is being owned by which transaction
	xidOfRow, ok := rowlock.rowToXid[waitedRow]
	// if row is not owned by any transaction, no circle
	if !ok {
		return false
	}
	return rowlock.dfs(xidOfRow)
}

// @description: 删除一个事务对应的所有依赖关系
func (rowlock *RowLock) Remove(xid uint64) {
	rowlock.lock.Lock()
	defer rowlock.lock.Unlock()

	for row, _ := range rowlock.xidToRow[xid] {
		// 从被该行阻塞的事务中选取一个恢复
		rowlock.selectTransaction(row)
	}
	// 删除事务持有的所有数据行
	delete(rowlock.xidToRow, xid)
	// 删除该事务等待的所有数据行
	delete(rowlock.xidToWait, xid)
	// 删除该事务的等待channel
	delete(rowlock.xidToWaitChan, xid)
}

// @description: 从被该行阻塞的事务中选取一个来持有数据行
func (rowlock *RowLock) selectTransaction(row int64) {
	// 删除持有该行数据的事务
	delete(rowlock.rowToXid, row)
	for xid := range rowlock.rowToWait[row] {
		delete(rowlock.rowToWait[row], xid)
		if _, ok := rowlock.xidToWait[xid]; !ok {
			continue
		}
		rowlock.rowToXid[row] = xid
		rowlock.xidToRow[xid][row] = true
		ch := rowlock.xidToWaitChan[xid]
		delete(rowlock.xidToWaitChan, xid)
		delete(rowlock.xidToWait, xid)
		ch <- true
		break
	}
	if len(rowlock.rowToWait) == 0 {
		delete(rowlock.rowToWait, row)
	}
}
