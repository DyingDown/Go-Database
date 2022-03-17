package ast

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Row struct {
	Pos  int64
	Size uint64
	Data []SQLValue
}

func (row *Row) String() string {
	var str string
	for _, v := range row.Data {
		str += fmt.Sprintf("%v\t", v)
	}
	return str
}

func (row *Row) GetPrimaryKey() SQLValue {
	return (*row).Data[0]
}

func (row *Row) SetRowData(indexs []int, values []SQLValue) {
	row.Data = values
	// for j, i := range indexs {
	// 	(*row).Data[i] = values[j]
	// }
	row.Size = uint64(len(row.Encode()))
}

func (row *Row) UpdateRow(indexs []int, values []SQLValue) {
	for i, j := range indexs {
		row.Data[j] = values[i]
	}
	row.Size = uint64(len(row.Encode()))
}
func (row *Row) SetMaxXid(xid uint64) {
	x := SQLInt(xid)
	row.Data[len(row.Data)-1] = &x
}

// encode row to bytes
func (row *Row) Encode() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, row.Pos)
	binary.Write(&buf, binary.BigEndian, uint16(len(row.Data)))
	for _, v := range row.Data {
		v.Encode(&buf)
	}
	return buf.Bytes()
}

// decode
func (row *Row) Decode(r io.Reader) {
	var length uint16
	binary.Read(r, binary.BigEndian, &row.Pos)
	binary.Read(r, binary.BigEndian, &length)
	row.Data = make([]SQLValue, length)
	for i := range row.Data {
		val, size, err := DecodeValue(r)
		if err != nil {
			panic(err)
		}
		row.Data[i] = val
		row.Size += size
	}
}
func (row *Row) SetPos(pos int64) {
	row.Pos = pos
}

func (row *Row) GetPos() int64 {
	return row.Pos
}

func (row *Row) MinXid() uint64 {
	return uint64(*row.Data[len(row.Data)-2].(*SQLInt))
}

func (row *Row) MaxXid() uint64 {
	return uint64(*row.Data[len(row.Data)-1].(*SQLInt))
}

func (row *Row) DeepCopy() []SQLValue {
	var data []SQLValue
	for _, v := range row.Data {
		data = append(data, v.DeepCopy())
	}
	return data
}
