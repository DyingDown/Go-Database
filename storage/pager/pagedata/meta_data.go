package pagedata

import (
	"bytes"
	"encoding/gob"
	"go-database/parser/ast"
	"io"
)

type MetaData struct {
	dirty   bool
	version string
	tables  map[string]*TableInfo
}

type TableInfo struct {
	TableId    uint32
	tableName  string
	Columns    []*ast.SQLColumnDefine
	FirstPage  uint32
	LastPage   uint32
	PrimaryKey uint32
}

func NewMetaData() *MetaData {
	return &MetaData{
		dirty:  false,
		tables: make(map[string]*TableInfo, 0),
	}
}

func (metadata *MetaData) Encode() []byte {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	encoder.Encode(metadata)
	return buf.Bytes()
}

func (metadata *MetaData) Decode(r io.Reader) error {
	decoder := gob.NewDecoder(r)
	return decoder.Decode(metadata)
}

func (metadata *MetaData) GetTableInfo(tableName string) *TableInfo {
	return metadata.tables[tableName]
}

func (metadata *MetaData) Size() int {
	return len(metadata.Encode())
}

func (metadata *MetaData) NewTableInfo(tableName string, cols []*ast.SQLColumnDefine) *TableInfo {
	return &TableInfo{
		TableId:   uint32(len(metadata.tables)),
		tableName: tableName,
		Columns:   cols,
	}
}

func (metadata *MetaData) AddTableInfo(tableInfo *TableInfo) {
	metadata.tables[tableInfo.tableName] = tableInfo
}

func (tableInfo *TableInfo) GetColumnInfo(columnName string) (int, *ast.SQLColumnDefine) {
	for index, column := range tableInfo.Columns {
		if columnName == column.ColumnName {
			return index, column
		}
	}
	return -1, nil
}

func (tableInfo *TableInfo) GetColumns() []*ast.SQLColumnDefine {
	return tableInfo.Columns
}
func (tableInfo *TableInfo) GetPrimaryKey() string {
	return tableInfo.Columns[tableInfo.PrimaryKey].ColumnName
}

// get column names
func (tableInfo *TableInfo) GetColumnNames() []string {
	columns := make([]string, 0)
	for _, column := range tableInfo.Columns {
		columns = append(columns, column.ColumnName)
	}
	return columns
}
