package lru

import (
	"container/list"
	"go-database/util"

	// "go-database/util/cache"
	"sync"
)

type LRU struct {
	size      int
	Pages     map[interface{}]*list.Element
	cacheList *list.List
	lock      sync.RWMutex
	expire    func(key util.KEY, value interface{})
}

type ListNode struct {
	key   util.KEY
	value interface{}
}

// var _ cache.Cache = (*LRU)(nil)

func NewLRU(cacheSize int) *LRU {
	return &LRU{
		size:      cacheSize,
		Pages:     make(map[interface{}]*list.Element),
		cacheList: list.New(),
	}
}

func (cache *LRU) AddExpire(f func(key util.KEY, value interface{})) {
	cache.expire = f
}
func (cache *LRU) GetData(key util.KEY) interface{} {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	if ele, ok := cache.Pages[key]; ok {
		cache.cacheList.MoveToFront(ele)
		return ele.Value.(*ListNode).value
	}
	return nil
}

func (cache *LRU) AddData(key util.KEY, newData interface{}) {
	cache.lock.Lock()
	defer cache.lock.Unlock()
	// if the cache is full
	if cache.cacheList.Len() == cache.size {
		// remove the last element
		ele := cache.cacheList.Remove(cache.cacheList.Back())
		delete(cache.Pages, ele.(*ListNode).key)
	}
	// add new data to cache
	ele := cache.cacheList.PushFront(&ListNode{key, newData})
	cache.Pages[key] = ele
}

func (cache *LRU) Close() {
	if cache.expire == nil {
		return
	}
	for i := cache.cacheList.Front(); i != nil; i = i.Next() {
		cache.expire(i.Value.(*ListNode).key, i.Value.(*ListNode).value)
	}

}
