package bplustree

import (
	"sort"
)

const degree = 255

type BPlusTreeNode struct {
	Keys        [degree]int
	Children    [degree + 1]int
	parent      int
	LeftAddr    int
	RightAddr   int
	isLeaf      bool
	CurrentAddr int
	num         int
}

func NewBPlusTreeNode(current int, leaf bool) BPlusTreeNode {
	node := BPlusTreeNode{
		isLeaf:      leaf,
	}
	return node
}

func (node *BPlusTreeNode) SearchNonLeaf(target int) int {
	pos := Lower_Bound(target, node.Keys, 0, node.num)
	return node.Children[pos]
}


// // find target data address
// func (node *BPlusTreeNode) SearchLeaf(target int) (pos int, flag bool) {
// 	pos = Lower_Bound(target, node.Keys, 0, node.num)
// 	if node.keys[pos] == target {
// 		flag = true
// 	} else {
// 		flag = false
// 	}
// }

func Lower_Bound(target int, keys [degree]int, left int, right int) int {
	for left < right {
		mid := (left + right) / 2
		if keys[mid] < target {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}
