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
	if node == nil || targetPos == node.Num || !bytes.Equal(node.Keys[targetPos], target) {
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
			for targetPos < node.Num && bytes.Equal(node.Keys[targetPos], target) {
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

func (tree *BPlusTree) insertInNode(key index.KeyType, value index.ValueType, node *BPlusTreeNode, index uint16) bool {
	// insert key
	copy(node.Keys[index+1:], node.Keys[index:node.order-1])
	node.Keys[index] = key

	// insert value
	if node.isLeaf {
		copy(node.Children[index+1:], node.Children[index:node.order])
		node.Children[index] = value
	} else {
		copy(node.Children[index+2:], node.Children[index+1:node.order])
		node.Children[index+1] = value
	}
	node.Num++
	return true
}

func (bplustree *BPlusTree) Insert(key index.KeyType, value index.ValueType) error {
	valueChan := bplustree.Search(key)
	for v := range valueChan {
		if bytes.Equal(value, v) {
			fmt.Println("value Already exisits")
			return nil
		}
	}
	node, index := bplustree.searchLowerInTree(key)
	bplustree.insertInNode(key, value, node, index)
	if node.Num >= bplustree.order {
		bplustree.splitLeaf(node)
	}
	return nil
}

func (tree *BPlusTree) splitLeaf(node *BPlusTreeNode) {
	// if is root, create new root
	var parentNode *BPlusTreeNode
	if node.CurrentAddr == tree.Root {
		parentNode = NewBPlusTreeNode(tree.order, false)
		parentPage := tree.pager.CreatePage(parentNode)
		parentNode.CurrentAddr = parentPage.PageNo
		parentNode.Num = 0
		parentNode.Children[0] = util.Uint32ToBytes(node.CurrentAddr)

		node.parent = parentNode.CurrentAddr
		tree.Root = parentNode.CurrentAddr
		tree.FirstLeaf = node.CurrentAddr
		tree.LastLeaf = node.CurrentAddr
	} else {
		parentNode, _ = tree.getNode(node.parent)
	}

	half := tree.order / 2
	// new node
	newNode := NewBPlusTreeNode(tree.order, true)
	newNode.Num = tree.order - half
	node.Num = half
	newNodePage := tree.pager.CreatePage(newNode)
	newNode.CurrentAddr = newNodePage.PageNo

	for i := half; i < tree.order; i++ {
		newNode.Keys[i-half] = node.Keys[i]
		newNode.Children[i-half] = node.Children[i]
	}

	// change neighbor relation
	// before change: node->rightNode newNode
	// after change : node->newNode->rightNode
	node.RightAddr = newNode.CurrentAddr
	newNode.LeftAddr = node.CurrentAddr
	newNode.RightAddr = node.RightAddr

	rightNode, err := tree.getNode(node.RightAddr)

	if err != nil {
		log.Errorf("fail to load neighbor node: %v", err)
		// update last leaf
		tree.LastLeaf = newNode.CurrentAddr
	} else {
		rightNode.LeftAddr = newNode.CurrentAddr
	}

	// change parent node
	pos := parentNode.Lower_Bound(newNode.Keys[0])
	tree.insertInNode(newNode.Keys[0], util.Uint32ToBytes(newNode.CurrentAddr), parentNode, pos)
	if parentNode.Num >= tree.order {
		// recursion in splitNoneLeaf()
		tree.splitNoneLeaf(parentNode)
	}
}

func (tree *BPlusTree) splitNoneLeaf(node *BPlusTreeNode) {
	// if is root, create new root
	var parentNode *BPlusTreeNode
	if node.CurrentAddr == tree.Root {
		parentNode = NewBPlusTreeNode(tree.order, false)
		parentPage := tree.pager.CreatePage(parentNode)
		parentNode.CurrentAddr = parentPage.PageNo
		parentNode.Num = 0
		parentNode.Children[0] = util.Uint32ToBytes(node.CurrentAddr)

		node.parent = parentNode.CurrentAddr
		tree.Root = parentNode.CurrentAddr
		tree.FirstLeaf = node.CurrentAddr
		tree.LastLeaf = node.CurrentAddr
	} else {
		parentNode, _ = tree.getNode(node.parent)
	}

	half := tree.order / 2
	// new node
	newNode := NewBPlusTreeNode(tree.order, true)
	newNode.Num = tree.order - half - 1
	node.Num = half
	newNodePage := tree.pager.CreatePage(newNode)
	newNode.CurrentAddr = newNodePage.PageNo

	// copy half node
	for i := half + 1; i < tree.order; i++ {
		newNode.Keys[i-half-1] = node.Keys[i]
		newNode.Children[i-half-1] = node.Children[i]
	}
	// one more child in non-leaf node than keys
	newNode.Children[newNode.Num] = node.Children[tree.order]

	// update child node, parent node relationships
	newNode.parent = parentNode.CurrentAddr

	for i := uint16(0); i < newNode.Num+1; i++ {
		child, err := tree.getNode(util.BytesToUInt32(newNode.Children[i]))
		if err != nil {
			log.Fatal("can't update new node's children's parent: %v", err)
		}
		child.parent = newNode.CurrentAddr
	}

	// splite parent node if need
	index := parentNode.Lower_Bound(newNode.Keys[0])
	tree.insertInNode(newNode.Keys[0],
		util.Uint32ToBytes(newNode.CurrentAddr),
		parentNode,
		index)
	if parentNode.Num >= tree.order {
		tree.splitNoneLeaf(parentNode)
	}
}
