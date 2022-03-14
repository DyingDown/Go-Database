package transporter

import "go-database/tbm"

type Request struct {
	Xid uint64
	SQL string
}

type Response struct {
	Xid        uint64
	ResultList *tbm.Result
	Err        string
}
