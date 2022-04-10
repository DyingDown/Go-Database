package wtinylfu

import (
	"go-database/util"
	"math"
)

type TwoNum uint8

var SEED []uint64 = []uint64{0xc3a5c85c97cb3127, 0xb492b66fbe98f273, 0x9ae16a3b2f90404f, 0xcbf29ce484222325}

const (
	upper uint8 = 0xF0
	lower uint8 = 0x0f
)

type CountMinSketch struct {
	cacheSize int
	tableSize int
	hashTable []TwoNum
	count     int
}

func NewCountMinSketch(cacheSize int) *CountMinSketch {
	size := int(-float64(cacheSize) * math.Log(0.03) / math.Ln2 * math.Ln2)
	return &CountMinSketch{
		cacheSize: cacheSize,
		tableSize: size,
		hashTable: make([]TwoNum, size/2),
	}
}

func (c *CountMinSketch) calcHash(key uint32, i int) uint64 {
	return (uint64(key) + SEED[i]) % uint64(c.tableSize*2)
}

func (c *CountMinSketch) Add(key util.KEY) {
	c.count++
	for i := 0; i < 4; i++ {
		hash := c.calcHash(key.Hash(), i)
		if hash&1 == 1 {
			c.hashTable[hash/2].AddB()
		} else {
			c.hashTable[hash/2].AddA()
		}
	}
}

func (c *CountMinSketch) GetMin(key util.KEY) uint8 {
	var minn uint8 = 18
	for i := 0; i < 4; i++ {
		hash := c.calcHash(key.Hash(), i)
		var cnt uint8
		if hash&1 == 1 {
			cnt = c.hashTable[hash/2].GetA()
		} else {
			cnt = c.hashTable[hash/2].GetB()
		}
		minn = min(minn, cnt)
	}
	return minn
}

func (c *CountMinSketch) Reset() {
	for _, v := range c.hashTable {
		v.SetA()
		v.SetB()
	}
}
func (tn *TwoNum) GetA() uint8 {
	return uint8(uint8(*tn) & upper)
}

func (tn *TwoNum) GetB() uint8 {
	return uint8(uint8(*tn) & lower)
}

func (tn *TwoNum) AddA() {
	if tn.GetA() >= 15 {
		return
	}
	*tn += 16
}

func (tn *TwoNum) AddB() {
	if tn.GetB() >= 15 {
		return
	}
	*tn += 1
}

func (tn *TwoNum) SetA() {
	A := tn.GetA()
	B := tn.GetB()
	A = A >> 1
	*tn = TwoNum(A | B)
}

func (tn *TwoNum) SetB() {
	A := tn.GetA()
	B := tn.GetB()
	B = B >> 1
	*tn = TwoNum(A | B)
}

func min(a, b uint8) uint8 {
	if a > b {
		return b
	}
	return a
}
