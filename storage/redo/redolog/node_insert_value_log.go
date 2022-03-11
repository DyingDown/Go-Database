package redolog

import (
	"bytes"
	"encoding/gob"
	"io"
)

type NodeInsertValueLog struct {
	logType    LogType
	lsn        int64
	tableId    uint32
	columnId   uint32
	PageNumber uint32
	Key        []byte
	Value      []byte
}

func NewNodeInsertValueLog(tableId uint32, columnId uint32, pageNumber uint32, key, value []byte) *NodeInsertValueLog {
	return &NodeInsertValueLog{
		logType:    NODE_INSERT_VALUE,
		tableId:    tableId,
		columnId:   columnId,
		PageNumber: pageNumber,
		Key:        key,
		Value:      value,
	}
}

func (log *NodeInsertValueLog) Encode() []byte {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(log)
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

func (log *NodeInsertValueLog) Decode(r io.Reader) error {
	decoder := gob.NewDecoder(r)
	return decoder.Decode(log)
}

func (log *NodeInsertValueLog) LSN() int64 {
	return log.lsn
}
