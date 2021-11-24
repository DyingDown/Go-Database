package pager

import (
	"Go-Database/storage/cache"
)

type Pager struct {
}

var cache_ cache.Cache

func (pager *Pager) LoadNode(addr int) (a interface{}) {
	cache_.getPage()
}

func (pager *Pager) NewNode(a interface{}) (addr int) {
}
