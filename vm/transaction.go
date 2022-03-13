/*
 * xid is start transaction id
 * SnapShot saved all active transaction
 * 		In order to solve nonrepeatable read, we need to save all active transaction
 *
 */
package vm

import "math"

const NULL_Xid int64 = math.MaxInt64

type Transaction struct {
	Xid      uint64
	SnapShot map[uint64]struct{}
}

func NewTransaction(xid uint64, trans map[uint64]*Transaction) *Transaction {
	transaction := &Transaction{}
	transaction.Xid = xid
	for k := range trans {
		transaction.SnapShot[k] = struct{}{}
	}
	return transaction
}
