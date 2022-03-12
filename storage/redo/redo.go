/*
 * - logFile: stores all the log information
 *
 * 	For one tranasction
 *		1. record all the log information,
 *		2. then apply changes to the page in memory
 *		3. before the changes actually fushed to disk, commit logs to logFile
 * 	This ensures that one transaction's log is complete
 *		if it broke, one transaction is either lost or stored in file
 */

package redo

import (
	"bytes"
	"go-database/storage/redo/redolog"
	"io"
	"log"
	"os"
	"sync"
)

type Redo struct {
	logFile  *os.File
	pageFile *os.File
	LSN      int64
	lock     sync.RWMutex
}

// @description: create a new redo log file
func Create(path string, pagefile *os.File) *Redo {
	// check if file already exists
	if status, err := os.Stat(path + "_log"); err == nil && status.Size() != 0 {
		log.Fatal("redo log file already exists")
		return Open(path, pagefile)
	}
	log, err := os.OpenFile(path+"_log", os.O_CREATE, 0666)
	if err != nil {
		panic("fail create file " + path + "_log")
	}
	return &Redo{
		logFile:  log,
		pageFile: pagefile,
		LSN:      0,
	}
}

// @description: open a redo log file
func Open(path string, pagefile *os.File) *Redo {
	log, err := os.OpenFile(path+"_log", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic("fail open file " + path + "_log")
	}
	return &Redo{
		logFile:  log,
		pageFile: pagefile,
	}
}

// @description: append one operation to log
func (redo *Redo) Append(log redolog.Log, w io.Writer) error {
	logBytes := log.Encode()
	// calc and update lsn
	log.SetLSN(redo.LSN)
	redo.LSN += int64(len(logBytes))

	_, err := w.Write(logBytes)
	if err != nil {
		return err
	}
	return nil
}

// @description: Write a group of log to log file
// @return: max LSN
func (redo *Redo) Write(log []redolog.Log) (int64, error) {
	// write to log file
	redo.lock.Lock()
	defer redo.lock.Unlock()
	buffer := bytes.NewBuffer(nil)
	for _, log := range log {
		err := redo.Append(log, buffer)
		if err != nil {
			return 0, err
		}
	}
	_, err := redo.logFile.Write(buffer.Bytes())
	if err != nil {
		return 0, err
	}
	err = redo.logFile.Sync()
	if err != nil {
		return 0, err
	}
	return redo.LSN, nil
}
