package redolog

import (
	"bytes"
	"encoding/gob"
	"io"
)

type SplitNodeLog struct {
	logType    LogType
	lsn        int64
	tableId    uint32
	columnId   uint32
	PageNumber uint32
}

func NewSplitNodeLog(tableId uint32, columnId uint32, pageNumber uint32) *SplitNodeLog {
	return &SplitNodeLog{
		logType:    SPLIT_NODE,
		tableId:    tableId,
		columnId:   columnId,
		PageNumber: pageNumber,
	}
}

func (log *SplitNodeLog) Encode() []byte {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(log)
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

func (log *SplitNodeLog) Decode(r io.Reader) error {
	decoder := gob.NewDecoder(r)
	return decoder.Decode(log)
}

func (log *SplitNodeLog) LSN() int64 {
	return log.lsn
}

func (log *SplitNodeLog) SetLSN(lsn int64) {
	log.lsn = lsn
}
