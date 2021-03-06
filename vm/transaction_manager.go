/*
 * A transaction is a group of indivisible operations
 * - start: create a transaction
 * - commit: commit changes
 * - abort: rollback changes
 */
package vm

import (
	"go-database/util"
	"log"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
)

// const (
// 	XID_FILE_NAME = ""
// )

type TransactionStatus byte

const (
	TS_START TransactionStatus = iota
	TS_COMMIT
	TS_ABORT
)

type TransactionManager struct {
	XidFile *os.File
	MaxXid  uint64
	lock    sync.RWMutex
}

// @description: create xid file
func CreateTM(path string) *TransactionManager {
	filepath := path + util.DBName + ".xid"
	// check if file already exists
	if status, err := os.Stat(filepath); err == nil && status.Size() != 0 {
		log.Fatal("xid file already exists")
		return OpenTM(path)
	}
	xid, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic("fail create file " + filepath)
	}
	// write max xid to head of file
	xid.WriteAt(util.Int64ToBytes(0), 0)
	xid.Sync()
	return &TransactionManager{
		XidFile: xid,
		MaxXid:  0,
	}
}

// @description: open xid file
func OpenTM(path string) *TransactionManager {
	filepath := path + util.DBName + ".xid"
	xid, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		logrus.Fatal("fail open file " + filepath)
	}
	// get max xid
	xidBytes := make([]byte, 8)
	n, err := xid.ReadAt(xidBytes, 0)
	if err != nil {
		logrus.Fatal("fail read file "+filepath+": %v", err)
	}
	// if the result is less than 8 bytes, then the xid is not complete, the file crashed
	if n != 8 {
		logrus.Fatal("fail read file " + filepath)
	}
	return &TransactionManager{
		XidFile: xid,
		MaxXid:  uint64(util.BytesToInt64(xidBytes)),
	}
}

// @description: close xid file
func (tm *TransactionManager) Close() {
	tm.XidFile.Close()
}

// @description: start a transaction
// @return: xid
func (tm *TransactionManager) Begin() uint64 {
	xid := tm.MaxXid
	tm.lock.Lock()
	tm.MaxXid++
	tm.lock.Unlock()
	tm.UpdateTransaction(xid, TS_START)
	tm.UpdateHeader(tm.MaxXid)
	return xid
}

// @description: commit a transaction
func (tm *TransactionManager) Commit(xid uint64) {
	tm.UpdateTransaction(xid, TS_COMMIT)
}

// @description: abort a transaction
func (tm *TransactionManager) Abort(xid uint64) {
	tm.UpdateTransaction(xid, TS_ABORT)
}

func (tm *TransactionManager) UpdateTransaction(xid uint64, status TransactionStatus) {
	_, err := tm.XidFile.WriteAt([]byte{byte(status)}, int64(xid)+8)

	if err != nil {
		logrus.Fatal("fail write file: %v", err)
	}
	err = tm.XidFile.Sync()
	if err != nil {
		logrus.Fatal("fail sync file: %v", err)
	}
}

// @description: update max xid in file
func (tm *TransactionManager) UpdateHeader(xid uint64) {
	_, err := tm.XidFile.WriteAt(util.Int64ToBytes(int64(xid)), 0)
	if err != nil {
		logrus.Fatal("fail write file " + tm.XidFile.Name())
	}
	err = tm.XidFile.Sync()
	if err != nil {
		logrus.Fatal("fail sync file " + tm.XidFile.Name())
	}
}

// @description: get transaction status
func (tm *TransactionManager) GetTransactionStatus(xid uint64) TransactionStatus {
	status := make([]byte, 1)
	_, err := tm.XidFile.ReadAt(status, int64(xid)+8)
	if err != nil {
		logrus.Fatal("fail read file " + tm.XidFile.Name())
	}
	return TransactionStatus(status[0])
}

// @description: check commited
func (tm *TransactionManager) CheckCommited(xid uint64) bool {
	status := tm.GetTransactionStatus(xid)
	// logrus.Infof("%v, status: %v", xid, status)
	return status == TS_COMMIT
}

// @description: check aborted
func (tm *TransactionManager) CheckAborted(xid uint64) bool {
	status := tm.GetTransactionStatus(xid)
	return status == TS_ABORT
}
