package bplustree

const order = 255

type BPlusTreeNode struct {
	Keys        [order]int
	Children    [order + 1]int
	parent      int
	LeftAddr    int
	RightAddr   int
	isLeaf      bool
	CurrentAddr int
	num         int
}

func NewBPlusTreeNode(current int, leaf bool) BPlusTreeNode {
	node := BPlusTreeNode{
		isLeaf: leaf,
	}
	return node
}

func (node *BPlusTreeNode) SearchNonLeaf(target int) int {
	pos := Lower_Bound(target, node.Keys, 0, node.num)
	return node.Children[pos]
}

func (node *BPlusTreeNode) Insert(target int, childAddr int) {
	pos := Lower_Bound(target, node.Keys, 0, node.num)
	for i := node.num - 1; i > pos; i-- {
		node.Keys[i] = node.Keys[i-1]
		if i-pos > 1 {
			node.Children[i] = node.Children[i-1]
		}
	}
	node.Keys[pos] = target
	node.Children[pos+1] = childAddr
}

func Lower_Bound(target int, keys [order]int, left int, right int) int {
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
