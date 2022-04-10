/*
 *	Cache:
 *		store the recently or frequently visited page
 *	LRU:
 *		1. LRU: Least Recent Used
 */
package cache

import (
	"go-database/util"
	"go-database/util/cache/lru"
	wtinylfu "go-database/util/cache/w-tinyLFU"
)

type Cache interface {
	AddData(key util.KEY, value interface{})
	GetData(key util.KEY) interface{}
	Close()
	AddExpire(f func(key util.KEY, value interface{}))
}

func CreateCache(cacheSize int) (ch Cache) {
	switch util.CacheType {
	case "LRU":
		return lru.NewLRU(cacheSize)
	case "WTinyLRU":
		return wtinylfu.NewWTinyLFU(cacheSize)
	}
	return ch
}
