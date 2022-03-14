package util

const (
	PageSize              = 8192      // 8K
	ActuralPageSize       = 8192 - 64 // 8K - 64
	Max_Paralled_Threads  = 4         // max number of paralled processes
	BPlusTreeKeyLen       = 8         // length of []byte of b+ tree node's key
	DoubleWriteBufferSize = 200       //  max page number double write's memory buffer can store
	NetWork               = "tcp"
	Address               = "127.0.0.1:8080"
)

var DBName = "test"
