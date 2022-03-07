package util

import (
	"go-database/parser/ast"

	log "github.com/sirupsen/logrus"
)

func BytesToUInt32(num []byte) uint32 {
	if len(num) != 4 {
		log.Errorf("The bytes is not a uint32 type")
		return 0
	}
	return uint32(num[0]) | uint32(num[1])<<8 | uint32(num[2])<<16 | uint32(num[3])<<24
}

func Uint32ToBytes(num uint32) []byte {
	return []byte{byte(num), byte(num >> 8), byte(num >> 16), byte(num >> 24)}
}

// check if sql value type mathches column type
func ValueTypeVsColumnType(valueType ast.SQLType, columnType ast.Types) bool {
	if columnType == ast.CT_INT {
		return valueType == ast.ST_INT
	} else if columnType == ast.CT_FLOAT {
		return valueType == ast.ST_FLOAT
	} else if columnType == ast.CT_STRING {
		return valueType == ast.ST_STRING
	}
	return false
}
