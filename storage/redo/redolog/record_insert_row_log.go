package redolog

import (
	"bytes"
	"encoding/gob"
	"go-database/parser/ast"
	"io"
)

type RecordInsertRowLog struct {
	logType    LogType
	lsn        int64
	PageNumber uint32
	row        ast.Row
}

func NewRecordInsertRowLog(pageNumber uint32, row ast.Row) *RecordInsertRowLog {
	return &RecordInsertRowLog{
		logType:    RECORD_INSERT_ROW,
		PageNumber: pageNumber,
		row:        row,
	}
}

func (log *RecordInsertRowLog) Encode() []byte {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(log)
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

func (log *RecordInsertRowLog) Decode(r io.Reader) error {
	decoder := gob.NewDecoder(r)
	return decoder.Decode(log)
}

func (log *RecordInsertRowLog) LSN() int64 {
	return log.lsn
}

func (log *RecordInsertRowLog) SetLSN(lsn int64) {
	log.lsn = lsn
}
