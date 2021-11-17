package bplustree

import (
	"Go-Database/storage/pager"
)

type BPlusTree struct {
	currentNode BPlusTreeNode
}

func (bplustree *BPlusTree) search(target int, pager pager.Pager) {
	// reach leaf node
	for bplustree.currentNode.isLeaf == false {
		nextAddr := bplustree.currentNode.SearchNonLeaf(target)
		pager.LoadNode(nextAddr)
	}
	
	// search in leaf node
	if bplustree.currentNode.SearchNonLeaf(target) {
		
	}
}
