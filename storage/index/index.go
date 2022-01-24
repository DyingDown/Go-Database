/*
 *	Index Manager
 *	index is tree node
 *	Index Manager manages tree nodes
 */
package index

// type of node's chidren
type KeyType []byte

// type of node's key
type ValueType []byte

type Index interface {
	Search(key KeyType) <-chan ValueType
	Insert(key KeyType, value ValueType) error
}
