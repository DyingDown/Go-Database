package bplustree

import (
	"fmt"
	"go-database/storage/pager"
)

type BPlusTree struct {
	Root *BPlusTreeNode
}

func (bplustree *BPlusTree) search(target int, pager pager.Pager) (int, *BPlusTreeNode) {
	// reach leaf node
	node := pager.LoadNode(bplustree.Root)
	for node.isLeaf == false {
		nextAddr := node.SearchNonLeaf(target)
		node = pager.LoadNode(nextAddr)
	}
	// search in leaf node
	targetPos := Lower_Bound(target, node.Keys, 0, node.num)
	if node.Keys[targetPos] == target {
		node = nil
	} else {
		fmt.Println("Dose not exist")
	}
	return targetPos, node
}

func (bplustree *BPlusTree) insert(target int, pager pager.Pager) {
	findAddr, node := bplustree.search(target, pager)
	if node != nil {
		// insert
		for i := order - 1; i >= findAddr+1; i-- {
			node.Keys[i] = node.Keys[i-1]
		}
		node.Keys[findAddr] = target
		node := new(BPlusTreeNode)
		node.num++
		if node.num == order {
			node = bplustree.splitLeaf(pager, node)
			for node.num == order {
				node = bplustree.splitNoneLeaf(pager, node)
			}
		}
	} else {
		fmt.Println("Already exists")
	}
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
