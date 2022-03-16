/*
 *	Cache:
 *		store the recently or frequently visited page
 *	TODOS:
 *		1. Select a strategy
 *		2. lru is not suitable for database
 */
package cache

type Cache struct {
	size  uint32
	Pages map[interface{}]interface{}
}

func CreateCache() *Cache {
	return &Cache{
		size:  50,
		Pages: make(map[interface{}]interface{}),
	}
}

func (cache *Cache) GetData(key interface{}) interface{} {
	// logrus.Infof("%t", key)
	if value, ok := cache.Pages[key]; ok { // if find
		return value
	}
	return nil
}

func (cache *Cache) AddData(key interface{}, newData interface{}) {
	if len(cache.Pages) == int(cache.size) {
		for k := range cache.Pages {
			delete(cache.Pages, k)
			break
		}
	}
	cache.Pages[key] = newData
}
