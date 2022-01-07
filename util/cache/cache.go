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
	if value, Exist := cache.Pages[key]; Exist { // if find
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
