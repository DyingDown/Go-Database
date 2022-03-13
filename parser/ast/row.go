package ast

import (
	"bytes"
	"encoding/binary"
	"io"
)

type Row struct {
	pos  uint64
	size uint64
	data []SQLValue
}

func (row *Row) GetPrimaryKey() SQLValue {
	return (*row).data[0]
}

func (row *Row) SetRowData(indexs []int, values []SQLValue) {
	for _, i := range indexs {
		(*row).data[i] = values[i]
	}
	row.size = uint64(len(row.Encode()))
}
func (row *Row) Data() []SQLValue {
	return row.data
}

func (row *Row) SetMaxXid(xid uint64) {
	x := SQLInt(xid)
	row.data[len(row.data)-1] = &x
}

// encode row to bytes
func (row *Row) Encode() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, row.pos)
	binary.Write(&buf, binary.BigEndian, len(row.data))
	for _, v := range row.data {
		v.Encode(&buf)
	}
	return buf.Bytes()
}

// decode
func (row *Row) Decode(r io.Reader) {
	var len int
	var pos uint64
	binary.Read(r, binary.BigEndian, &pos)
	row.pos = pos
	binary.Read(r, binary.BigEndian, &len)
	row.data = make([]SQLValue, len)
	for i := range row.data {
		val, size, err := DecodeValue(r)
		if err != nil {
			panic(err)
		}
		row.data[i] = val
		row.size += size
	}
}
func (row *Row) Size() int64 {
	return int64(row.size)
}

func (row *Row) SetPos(pos uint64) {
	row.pos = pos
}

func (row *Row) GetPos() uint64 {
	return row.pos
}

func (row *Row) MinXid() uint64 {
	return uint64(*row.data[len(row.data)-2].(*SQLInt))
}

func (row *Row) MaxXid() uint64 {
	return uint64(*row.data[len(row.data)-1].(*SQLInt))
}
