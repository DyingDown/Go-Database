/*
 * logFile stores the following format:
 * 		1.	LSN
 * 		2.	page number
 * 		3.	page data
 * 		4.	check sum
 */

package redo

type Log interface {
	Encode() []byte
	LSN(int64) int64
}
