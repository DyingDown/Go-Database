package bplustree

import (
	"bytes"
	"fmt"
	"go-database/storage/index"
	"go-database/storage/pager"
	"go-database/util"

	log "github.com/sirupsen/logrus"
)

type BPlusTree struct {
	Root      uint32
	FirstLeaf uint32
	LastLeaf  uint32
	KeySize   uint8
	ValueSize uint8
	order     uint16
	pager     *pager.Pager
}

func NewBPlusTree(pager *pager.Pager, keySize uint8, valueSize uint8) *BPlusTree {
	order := uint16(util.PageSize-16) / (uint16(keySize + valueSize))
	rootNode := NewBPlusTreeNode(order, true)
	rootPage := pager.CreatePage(rootNode)
	rootNode.CurrentAddr = rootPage.PageNo
	rootNode.parent = 0
	rootNode.LeftAddr = 0
	rootNode.RightAddr = 0
	rootNode.Num = 0
	rootNode.Keys = make([]index.KeyType, order)
	rootNode.Children = make([]index.ValueType, order+1)
	return &BPlusTree{
		Root:      rootNode.CurrentAddr,
		pager:     pager,
		FirstLeaf: rootNode.CurrentAddr,
		LastLeaf:  rootNode.CurrentAddr,
		KeySize:   keySize,
		ValueSize: valueSize,
		order:     order,
	}
}

func (bplustree *BPlusTree) getNode(pageNum uint32) (*BPlusTreeNode, error) {
	node := &BPlusTreeNode{
		tree: bplustree,
	}
	_, err := bplustree.pager.GetPage(pageNum, node)
	return node, err
}
func (bplustree *BPlusTree) searchLowerInTree(target index.KeyType) (*BPlusTreeNode, uint16) {
	node, err := bplustree.getNode(bplustree.Root)
	if err != nil {
		log.Errorf("fail to load tree node: %v", err)
		return nil, 0
	}

	// reach leaf node
	for node.isLeaf == false {
		nextAddr := node.SearchNonLeaf(target)
		node, err = bplustree.getNode(util.BytesToUInt32(nextAddr))
		if err != nil {
			log.Fatal(err)
			return nil, 0
		}
	}

	// search in leaf node
	targetPos := node.Lower_Bound(target)
	return node, targetPos
}
func (bplustree *BPlusTree) Search(target index.KeyType) <-chan index.ValueType {
	ValueChan := make(chan index.ValueType, 100)
	var err error
	node, targetPos := bplustree.searchLowerInTree(target)

	// verify targetPos
	if node == nil || targetPos == node.Num || bytes.Compare(node.Keys[targetPos], target) != 0 {
		node = nil
		close(ValueChan)
	}
	nodePageNo := util.BytesToUInt32(node.Children[targetPos])
	if nodePageNo == 0 {
		close(ValueChan)
		return ValueChan
	}

	// put data into ValueChan
	go func() {
		defer close(ValueChan)
		for {
			// search next child
			for targetPos < node.Num && bytes.Compare(node.Keys[targetPos], target) == 0 {
				ValueChan <- node.Children[targetPos]
				targetPos++
			}
			// search next node
			if targetPos == node.Num {
				if node.RightAddr == 0 {
					break
				}
				node, err = bplustree.getNode(node.RightAddr)
				if err != nil {
					log.Fatal(err)
				}
			} else {
				break
			}
		}
	}()
	return ValueChan
}

func (bplustree *BPlusTree) Insert(key index.KeyType, value index.ValueType) {
	valueChan := bplustree.Search(key)

	// if node != nil {
	// 	// insert
	// 	for i := order - 1; i >= findAddr+1; i-- {
	// 		node.Keys[i] = node.Keys[i-1]
	// 	}
	// 	node.Keys[findAddr] = target
	// 	node := new(BPlusTreeNode)
	// 	node.num++
	// 	if node.num == order {
	// 		node = bplustree.splitLeaf(pager, node)
	// 		for node.num == order {
	// 			node = bplustree.splitNoneLeaf(pager, node)
	// 		}
	// 	}
	// } else {
	// 	fmt.Println("Already exists")
	// }
}

func (bplustree *BPlusTree) splitLeaf(pager pager.Pager, node *BPlusTreeNode) *BPlusTreeNode {
	// if is root, create new root
	var parentNode *BPlusTreeNode
	if node == bplustree.Root {
		parentNode = new(BPlusTreeNode)
		parentNode.CurrentAddr = pager.NewNode(parentNode)
		parentNode.isLeaf = false
		parentNode.num = 0
		parentNode.Children[0] = node.CurrentAddr

		node.parent = parentNode.CurrentAddr
		bplustree.Root = parentNode
	} else {
		parentNode = pager.LoadNode(node.parent)
	}
	half := order / 2
	// new node
	newNode := node
	newNode.num = order - half
	node.num = half
	newNode.CurrentAddr = pager.NewNode(newNode)
	for i := half; i < order; i++ {
		newNode.Keys[i-half] = node.Keys[i]
		newNode.Children[i-half] = node.Children[i]
	}
	// change neigbor relation
	newNode.RightAddr = node.RightAddr
	rightNode := pager.LoadNode(node.RightAddr)
	rightNode.LeftAddr = newNode.CurrentAddr

	node.RightAddr = newNode.CurrentAddr
	newNode.LeftAddr = node.CurrentAddr
	parentNode.Insert(newNode.Children[0], newNode.CurrentAddr)
	return parentNode
}

func (bplustree *BPlusTree) splitNoneLeaf(pager pager.Pager, node *BPlusTreeNode) *BPlusTreeNode {
	// if is root, create new root
	var parentNode *BPlusTreeNode
	if node == bplustree.Root {
		parentNode = new(BPlusTreeNode)
		parentNode.CurrentAddr = pager.NewNode(parentNode)
		parentNode.isLeaf = false
		parentNode.num = 0
		parentNode.Children[0] = node.CurrentAddr

		node.parent = parentNode.CurrentAddr
		bplustree.Root = parentNode
	} else {
		parentNode = pager.LoadNode(node.parent)
	}
	half := order / 2
	// new node
	newNode := node
	newNode.num = order - half - 1
	node.num = half
	newNode.CurrentAddr = pager.NewNode(newNode)
	targetVal := node.Children[half]
	for i := half + 1; i < order; i++ {
		newNode.Keys[i-half-1] = node.Keys[i]
		newNode.Children[i-half-1] = node.Children[i]
	}
	// one more child in non-leaf node than keys
	newNode.Children[newNode.num] = node.Children[order]

	parentNode.Insert(targetVal, newNode.CurrentAddr)
	return parentNode
}
