/*
 * This package is mainly used to solve import cycle error
 */

package recovery

import (
	"go-database/storage/redo/redolog"
	"io"
)

type Btree interface {
	Search(key []byte) <-chan []byte
	Insert(key []byte, value []byte) error
}

type Log interface {
	Encode() []byte
	Decode(r io.Reader) error
	LSN() int64
}

type GeneralLog struct {
	Log Log
}

func (log *GeneralLog) ToNodeInsert() *redolog.NodeInsertValueLog {
	return log.Log.(*redolog.NodeInsertValueLog)
}

func (log *GeneralLog) ToSplitNode() *redolog.SplitNodeLog {
	return log.Log.(*redolog.SplitNodeLog)
}
