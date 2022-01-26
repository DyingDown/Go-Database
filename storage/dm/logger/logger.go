package logger

import (
	"go-database/util"
	"os"
)

type Logger struct {
	file     *os.File
	fileSize uint64
	checkSum uint32
}

func CreateLogger(path string) *Logger {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic("fail create log file " + path)
	}
	return &Logger{
		file: f,
	}
}

func OpenLogger(path string) *Logger {
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		panic("fail open log file " + path)
	}
	fileInfo, _ := f.Stat()
	if fileInfo.Size() < 4 {
		panic("Not a legal file")
	}
	var check = make([]byte, 4)
	_, err = f.WriteAt(check, 0)
	if err != nil {
		panic("Can't read checkSum")
	}
	return &Logger{
		file:     f,
		fileSize: uint64(fileInfo.Size()),
		checkSum: util.BytesToUInt32(check),
	}
}

func (logger *Logger) Close() error {
	err := logger.file.Close()
	if err != nil {
		return err
	}
	return nil
}
