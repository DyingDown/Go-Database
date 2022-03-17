/*
 *	Cache:
 *		store the recently or frequently visited page
 *	LRU:
 *		1. LRU: Least Recent Used
 */
package cache

import (
	"container/list"
	"sync"
)

type Cache struct {
	size      int
	Pages     map[interface{}]*list.Element
	cacheList *list.List
	lock      sync.RWMutex
	expire    func(key, value interface{})
}

type ListNode struct {
	key   interface{}
	value interface{}
}

func CreateCache() *Cache {
	return &Cache{
		size:      50,
		Pages:     make(map[interface{}]*list.Element),
		cacheList: list.New(),
	}
}

func (cache *Cache) AddExpire(f func(key, value interface{})) {
	cache.expire = f
}
func (cache *Cache) GetData(key interface{}) interface{} {
	cache.lock.RLock()
	defer cache.lock.RUnlock()
	if ele, ok := cache.Pages[key]; ok {
		cache.cacheList.MoveToFront(ele)
		return ele.Value.(*ListNode).value
	}
	return nil
}

func (cache *Cache) AddData(key interface{}, newData interface{}) {
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

func (cache *Cache) Close() {
	if cache.expire == nil {
		return
	}
	for i := cache.cacheList.Front(); i != nil; i = i.Next() {
		cache.expire(i.Value.(*ListNode).key, i.Value.(*ListNode).value)
	}

}
