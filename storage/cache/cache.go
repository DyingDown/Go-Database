package cache

type Object interface{}

type Cache struct {
	size  int
	Pages map[Object]*ListNode
}

type ListNode struct {
	Prev *ListNode
	Data Object
	Next *ListNode
}

func (cache *Cache) getPage(addr interface{}) {
	if _, Exist := cache.Pages[addr]; Exist { // if find
		cache.Pages[addr].Prev.Next = cache.Pages[addr].Next
		cache.Pages[addr].Next.Prev = cache.Pages[addr].Prev
	} else {

	}
}
