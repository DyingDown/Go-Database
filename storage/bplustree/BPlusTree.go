package bplustree

import (
	"Go-Database/storage/pager"
	"fmt"
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
	// dataAddr, isFind := bplustree.currentNode.SearchLeaf(target)
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
		for node.num == order {
			node = bplustree.splitNode(pager, node)
		}
	} else {
		fmt.Println("Already exists")
	}
}

func (bplustree *BPlusTree) splitNode(pager pager.Pager, node *BPlusTreeNode) *BPlusTreeNode {
	// if is root, create new root
	var parentNode *BPlusTreeNode
	if node == bplustree.Root {
		parentNode = new(BPlusTreeNode)
		parentAddr := pager.NewNode(parentNode)
		parentNode.CurrentAddr = parentAddr
		node.parent = parentAddr
		bplustree.Root = parentNode
	} else {
		parentNode = pager.LoadNode(node.parent)
	}
	// new node
	newNode := node
	half := order / 2
	newNode.num = order - half
	newNodeAddr := pager.NewNode(newNode)
	for i := half; i < order; i++ {
		newNode.Keys[i-half] = node.Keys[i]
		node.Keys[i] = 0
	}
	for i := order - half - 1; i < order; i++ {
		newNode.Keys[i] = 0
	}
	newNode.RightAddr = node.RightAddr
	rightNode := pager.LoadNode(node.RightAddr)
	rightNode.LeftAddr = newNodeAddr

	node.RightAddr = newNodeAddr
	newNode.LeftAddr = node

	return parentNode
}
