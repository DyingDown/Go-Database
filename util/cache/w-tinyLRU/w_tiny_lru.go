/*
 *	w-tiny-lfu contains three module
 * 	window:
 *		first store the data in windows.
 *		it's a very small lru
 *		Get(): if element in it is used: move to front
 *		Add(): if insert a data to window
 *			if it's full before insertion:
 *				append the last element to probation head
 *				then add to head
 *			else just add to head
 *	probation:
 *		probation is also a lru like cache
 *		Get(): get data and then move the data to protection
 *		Add(): insert data to probation
 * 			if it's oversize after insertion
 * 				compare the tail element with head element: Compare(victim, candidate)
 *				delete one based on Compare(,) result
 *	protection:
 *		a lru
 *		Get(): get data and move to head
 *		Add(): insert data to protection
 *			if it's oversize after insertion:
 * 				delete tail and move tail to probation
 *  frequency of data D: f(D):
 *		count min sketch return a frequency
 *  Compare(victim, candidate):
 * 		if f(candidate) < f(victim):
 *			throw candidate
 * 		if f(candidate) <= 5:
 *			throw candidate
 *      else: throw victim
 */
package wtinylru

import (
	"container/list"
	"go-database/util"

	// "go-database/util/cache"
	"math/rand"
	"sync"

	"github.com/sirupsen/logrus"
)

type ListNode struct {
	key   util.KEY
	value interface{}
}

type WTinyLFU struct {
	// window lru size = totalsize * 2%
	windowSize int
	windowList *list.List
	windowMap  map[util.KEY]*list.Element
	// probation lru size = totalsize * 20%
	probationSize int
	probationList *list.List
	probationMap  map[util.KEY]*list.Element
	// protection lru size = totalsize * 80%
	protectionSize int
	protectionList *list.List
	protectionMap  map[util.KEY]*list.Element

	frequency *CountMinSketch
	lock      sync.Mutex

	expire func(key util.KEY, value interface{})
}

// var _ cache.Cache = (*WTinyLRU)(nil)

func NewWTinyLFU(cachesize int) *WTinyLFU {
	if cachesize <= 100 {
		cachesize = 100
	}
	wsize := cachesize * 2 / 100
	psize := cachesize * 2 / 10
	ptsize := cachesize - wsize - psize
	return &WTinyLFU{
		windowSize:     wsize,
		windowList:     list.New(),
		windowMap:      make(map[util.KEY]*list.Element),
		probationSize:  psize,
		probationList:  list.New(),
		probationMap:   make(map[util.KEY]*list.Element),
		protectionSize: ptsize,
		protectionList: list.New(),
		protectionMap:  make(map[util.KEY]*list.Element),
		frequency:      NewCountMinSketch(cachesize),
	}
}

func (w *WTinyLFU) AddInWindow(key util.KEY, value interface{}) {
	ele := w.windowList.PushFront(&ListNode{key, value})
	w.windowMap[key] = ele
	if w.windowList.Len() > w.windowSize {
		ele := w.windowList.Remove(w.windowList.Back())
		delete(w.windowMap, ele.(*ListNode).key)
		// add deleted element to probation
		w.AddInProbation(ele.(*ListNode).key, ele.(*ListNode).value)
	}
}

func (w *WTinyLFU) AddInProtection(key util.KEY, value interface{}) {
	ele := w.protectionList.PushFront(&ListNode{key, value})
	w.protectionMap[key] = ele
	if w.protectionList.Len() > w.protectionSize {
		ele := w.protectionList.Remove(w.protectionList.Back())
		delete(w.protectionMap, ele.(*ListNode).key)
		// add deleted element to probation
		w.AddInProbation(ele.(*ListNode).key, ele.(*ListNode).value)
	}
}

func (w *WTinyLFU) AddInProbation(key util.KEY, value interface{}) {
	ele := w.probationList.PushFront(&ListNode{key, value})
	w.probationMap[key] = ele
	if w.probationList.Len() > w.protectionSize {
		// victim is the back, candidate is front
		if w.compare(w.probationList.Back(), w.probationList.Front()) {
			// throw candidate
			ele := w.probationList.Remove(w.probationList.Front())
			delete(w.probationMap, ele.(*ListNode).key)
		} else {
			// throw victim
			ele := w.probationList.Remove(w.probationList.Back())
			delete(w.probationMap, ele.(*ListNode).key)
		}
	}
}

func (w *WTinyLFU) GetData(key util.KEY) interface{} {
	// check if data is in window
	data, ok := w.windowMap[key]
	if ok {
		w.windowList.MoveToFront(data)
		return data.Value.(*ListNode).value
	}
	// check if data in probation
	data, ok = w.probationMap[key]
	if ok {
		w.AddInProtection(key, data.Value.(*ListNode).value)
		delete(w.probationMap, key)
		return data.Value.(*ListNode).value
	}
	// check if data in protection
	data, ok = w.protectionMap[key]
	if ok {
		w.protectionList.MoveToFront(data)
		return data.Value.(*ListNode).value
	}
	// data not in cache
	return nil
}

func (w *WTinyLFU) AddData(key util.KEY, data interface{}) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if data := w.GetData(key); data != nil {
		logrus.Error("data already exist")
		return
	}
	ele := w.windowList.PushFront(&ListNode{key, data})
	w.windowMap[key] = ele
	if w.windowList.Len() > w.windowSize {
		ele := w.windowList.Remove(w.windowList.Back())
		delete(w.windowMap, ele.(*ListNode).key)
		w.AddInProbation(ele.(*ListNode).key, ele.(*ListNode).value)
	}
}

func (w *WTinyLFU) getFrequency(key util.KEY) uint8 {
	return w.frequency.GetMin(key)
}

// @return: trow candiate(true) or not
func (w *WTinyLFU) compare(victim *list.Element, candidate *list.Element) bool {
	victimFreq := w.getFrequency(victim.Value.(*ListNode).key)
	candidateFreq := w.getFrequency(candidate.Value.(*ListNode).key)
	if candidateFreq < victimFreq {
		return true
	} else if candidateFreq <= 5 {
		return true
	}
	return rand.Intn(2) == 0
}

func (w *WTinyLFU) AddExpire(f func(key util.KEY, value interface{})) {
	w.expire = f
}

func (w *WTinyLFU) Close() {
	if w.expire == nil {
		return
	}
	for i := w.windowList.Front(); i != nil; i = i.Next() {
		w.expire(i.Value.(*ListNode).key, i.Value.(*ListNode).value)
	}
	for i := w.probationList.Front(); i != nil; i = i.Next() {
		w.expire(i.Value.(*ListNode).key, i.Value.(*ListNode).value)
	}
	for i := w.protectionList.Front(); i != nil; i = i.Next() {
		w.expire(i.Value.(*ListNode).key, i.Value.(*ListNode).value)
	}
}
