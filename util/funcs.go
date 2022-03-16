package util

import (
	"encoding/binary"
	"math"
	"unicode"
)

func BytesToUInt32(num []byte) uint32 {
	return binary.BigEndian.Uint32(num)
	// if len(num) != 4 {
	// 	log.Errorf("The bytes is not a uint32 type")
	// 	return 0
	// }
	// return uint32(num[0]) | uint32(num[1])<<8 | uint32(num[2])<<16 | uint32(num[3])<<24
}

func Uint32ToBytes(num uint32) []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, num)
	return bytes
}

func Int64ToBytes(num int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(num))
	return bytes
}

func BytesToInt64(bytes []byte) int64 {
	return int64(binary.BigEndian.Uint64(bytes))
}

func Float64ToBytes(num float64) []byte {
	bits := math.Float64bits(num)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}

func CheckSum(bytes []byte) []byte {
	var sum []byte = make([]byte, 4)
	// check sum dose not include the last four bytes(check sum)
	for _, b := range bytes[:len(bytes)-4] {
		sum[b%4] ^= bytes[b]
	}
	return sum
}

func LSN(bytes []byte) int64 {
	return int64(BytesToInt64(bytes[14:22]))
}

func IsPunct(b byte) bool {
	return b == '+' || b == '-' || b == '/' || b == '=' || b == '>' || b == '<' || unicode.IsPunct(rune(b))
}
