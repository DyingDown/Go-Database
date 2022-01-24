package index

type ValueType []byte
type KeyType []byte

type Index interface {
	Search(key KeyType) <-chan ValueType
	Insert(key KeyType, value ValueType) error
}
