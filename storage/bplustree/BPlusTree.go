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
	targetPos = Lower_Bound(target, node.Keys, 0, node.num)
	if bplustree.currentNode.keys[pos] == target {
		flag = true
	} else {
		fmt.Println("Dose not exist")
		flag = false
	}
}

func (bplustree *bplustree) insert(target int, pager pager.Pager) {
	findAddr, isFind := bplustree.search(target, pager)
	if isFind == false {
		bplustree.currentNode.Keys = append(bplustree.currentNode.Keys[:findAddr], append([]int{target}, bplustree.currentNode.Keys[findAddr:]...)...)
		bplustree.currentNode.num ++
		for bplustree.currentNode.num == degree {
			newLeftKeys = 
		}
	} else {
		fmt.Println("Already exists")
	}
}

func (bplustree *BPlusTree) splitNode() (newNode BPlusTreeNode, newNodeAddr int){
	newNode = bplustree.currentNode
	newNode.Keys = keys[]
}
