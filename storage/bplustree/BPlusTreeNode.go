package bplustree

import (
	"bytes"
	"encoding/binary"
	"go-database/storage/index"
	"io"
)

type BPlusTreeNode struct {
	CurrentAddr uint32
	parent      uint32
	LeftAddr    uint32
	RightAddr   uint32
	isLeaf      bool
	order       uint16
	Num         uint16
	Keys        []index.KeyType
	Children    []index.ValueType
	tree        *BPlusTree
}

func NewBPlusTreeNode(order uint16, leaf bool) *BPlusTreeNode {
	return &BPlusTreeNode{
		isLeaf:   leaf,
		order:    order,
		Keys:     make([]index.KeyType, order),
		Children: make([]index.ValueType, order+1),
	}
}

func (node *BPlusTreeNode) SearchNonLeaf(target index.KeyType) index.ValueType {
	pos := Lower_Bound(target, node.Keys, 0, node.Num)
	return node.Children[pos]
}

func (node *BPlusTreeNode) Insert(target index.KeyType, child index.ValueType) {
	pos := Lower_Bound(target, node.Keys, 0, node.Num)
	for i := node.Num - 1; i > pos; i-- {
		node.Keys[i] = node.Keys[i-1]
		if i-pos > 1 || node.isLeaf {
			node.Children[i] = node.Children[i-1]
		}
	}
	node.Keys[pos] = target
	if node.isLeaf {
		node.Children[pos+1] = child
	} else {
		node.Children[pos] = child
	}
}

func Lower_Bound(target index.KeyType, keys []index.KeyType, left uint16, right uint16) uint16 {
	for left < right {
		mid := (left + right) / 2
		if compare(keys[mid], target) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

func compare(a, b index.KeyType) int {
	return bytes.Compare(a, b)
}

func (node *BPlusTreeNode) Size() int {
	keySize := int(node.Num) * len(node.Keys[0])
	valueSize := int(node.Num+1) * len(node.Children[0])
	return 4 + 4 + 4 + 4 + 1 + 2 + 2 + keySize + valueSize
}

func (node *BPlusTreeNode) Encode() []byte {
	var buf bytes.Buffer
	int32Buff := make([]byte, 4)
	binary.BigEndian.PutUint32(int32Buff, node.CurrentAddr)
	buf.Write(int32Buff)
	binary.BigEndian.PutUint32(int32Buff, node.parent)
	buf.Write(int32Buff)
	binary.BigEndian.PutUint32(int32Buff, node.LeftAddr)
	buf.Write(int32Buff)
	binary.BigEndian.PutUint32(int32Buff, node.RightAddr)
	buf.Write(int32Buff)
	if node.isLeaf {
		buf.Write([]byte{1})
	} else {
		buf.Write([]byte{0})
	}
	// order is not saved because order is saved in tree
	int16Buff := make([]byte, 2)
	binary.BigEndian.PutUint16(int16Buff, node.Num)
	buf.Write(int16Buff)
	// Keys and Children's Length is not fixed
	for i := 0; i < int(node.Num); i++ {
		buf.Write(node.Keys[i][:])
		buf.Write(node.Children[i][:])
	}
	if node.isLeaf {
		buf.Write(node.Children[node.Num][:])
	}
	return buf.Bytes()
}

func (node *BPlusTreeNode) Decode(r io.Reader) error {
	int32Buff := make([]byte, 4)
	r.Read(int32Buff)
	node.CurrentAddr = binary.BigEndian.Uint32(int32Buff)
	r.Read(int32Buff)
	node.parent = binary.BigEndian.Uint32(int32Buff)
	r.Read(int32Buff)
	node.LeftAddr = binary.BigEndian.Uint32(int32Buff)
	r.Read(int32Buff)
	node.RightAddr = binary.BigEndian.Uint32(int32Buff)
	oneByteBuff := make([]byte, 1)
	r.Read(oneByteBuff)
	if oneByteBuff[0] == 1 {
		node.isLeaf = true
	} else {
		node.isLeaf = false
	}
	int16Buff := make([]byte, 2)
	r.Read(int16Buff)
	node.Num = binary.BigEndian.Uint16(int16Buff)
	// read keys and children
	node.Keys = make([]index.KeyType, int(node.tree.order))
	if node.isLeaf {
		node.Children = make([]index.ValueType, node.tree.order)
	} else {
		node.Children = make([]index.ValueType, node.tree.order+1)
	}
	for i := 0; i < int(node.Num); i++ {
		node.Keys[i] = make(index.KeyType, node.tree.KeySize)
		r.Read(node.Keys[i][:])
		node.Children[i] = make(index.ValueType, node.tree.ValueSize)
		r.Read(node.Children[i][:])
	}
	for node.isLeaf {
		node.Children[node.Num] = make(index.ValueType, node.tree.ValueSize)
		r.Read(node.Children[node.Num][:])
	}
	return nil
}
