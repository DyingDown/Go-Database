package redolog

import (
	"bytes"
	"encoding/gob"
	"io"
)

type CreatePageLog struct {
	logType    LogType
	lsn        int64
	PageNumber uint32
}

func NewCreatePageLog(pageNumber uint32) *CreatePageLog {
	return &CreatePageLog{
		logType:    CREATE_PAGE,
		PageNumber: pageNumber,
	}
}

func (log *CreatePageLog) Encode() []byte {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(log)
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

func (log *CreatePageLog) Decode(r io.Reader) error {
	decoder := gob.NewDecoder(r)
	return decoder.Decode(log)
}

func (log *CreatePageLog) LSN() int64 {
	return log.lsn
}

func (log *CreatePageLog) SetLSN(lsn int64) {
	log.lsn = lsn
}
