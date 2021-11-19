package bplustree

import (
	"Go-Database/storage/pager"
	"fmt"
)

type BPlusTree struct {
	currentNode BPlusTreeNode
}

func (bplustree *BPlusTree) search(target int, pager pager.Pager) (targetPos int, isFind bool) {
	// reach leaf node
	for bplustree.currentNode.isLeaf == false {
		nextAddr := bplustree.currentNode.SearchNonLeaf(target)
		bplustree.currentNode = pager.LoadNode(nextAddr)
	}
	// search in leaf node
	// dataAddr, isFind := bplustree.currentNode.SearchLeaf(target)
	targetPos = Lower_Bound(target, bplustree.currentNode.Keys, 0, bplustree.currentNode.num)
	if bplustree.currentNode.Keys[targetPos] == target {
		isFind = true
	} else {
		fmt.Println("Dose not exist")
		isFind = false
	}
}

func (bplustree *BPlusTree) insert(target int, pager pager.Pager) {
	findAddr, isFind := bplustree.search(target, pager)
	if isFind == false {
		bplustree.currentNode.Keys[bplustree.currentNode.num] = target
		bplustree.currentNode.num++
		for bplustree.currentNode.num == order {
			newNode, newAddr := bplustree.splitNode(pager)
			newNode.parent = bplustree.currentNode.parent
			bplustree.currentNode = pager.LoadNode(bplustree.currentNode.parent)
			target = newNode.Keys[0]

		}
	} else {
		fmt.Println("Already exists")
	}
}

func (bplustree *BPlusTree) splitNode(pager pager.Pager) (newNode BPlusTreeNode, newNodeAddr int) {
	newNode = bplustree.currentNode
	half := order / 2
	for i := half; i < order; i++ {
		newNode.Keys[i-half] = bplustree.currentNode.Keys[i]
		bplustree.currentNode.Keys[i] = 0

	}
	for i := order - half - 1; i < order; i++ {
		newNode.Keys[i] = 0
	}
	return newNode, pager.NewNode()
}
