/*
 * logFile stores the following format:
 * 		1.	LSN
 * 		2.	page number
 * 		3.	page data
 * 		4.	check sum
 */

package redolog

import (
	"io"
)

type LogType uint8

const (
	SPLIT_NODE LogType = iota
	NODE_INSERT_VALUE
	RECORD_INSERT_ROW
	RECORD_DPDATE_XMAX
	META_UPDATE
)

type Log interface {
	Encode() []byte
	Decode(r io.Reader)
	LSN(int64) int64
}
