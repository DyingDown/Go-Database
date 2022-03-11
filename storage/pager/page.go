/*
 *	All kinds of datas are stored in Page Struct
 *  There are tree types of data:
 *  	1. Meta:  stores the meta information of sql file, tables, and ect.
 *		2. Data:  stores rows of tables
 *		3. Index: stores b+ tree node
 *	Page also have some meta information to Uniquely identify page or to help select page
 *  The size of the page is const, and it's defined in uil package
 *	Page is controled directly by pager
 *
 *	LSN(long sequence number): Identifies where a particular log file is recorded in the log file
 */

package pager

import (
	"bytes"
	"encoding/binary"
	"go-database/storage/recovery"
	"go-database/util"
	"io"
	"sync"

	log "github.com/sirupsen/logrus"
)

type PageType uint8

const (
	MetaPage = iota
	DataPage
	IndexPage
)

// type PageData
type PageData interface {
	Encode() []byte         // to []bytes
	Decode(io.Reader) error // to struct
	Size() int
}

type Page struct {
	// currently has no use
	pageType PageType
	// PageNo*PageSize is the offset relative to the head of file
	// PageSize if a const
	PageNo     uint32 // page number
	prevPageNo uint32
	nextPageNo uint32
	dirty      bool
	LSN        int64    // long sequence number
	pageData   PageData // content of page
	lock       sync.RWMutex
	Logs       []recovery.Log
}

func NewPage(pageNo uint32, data PageData) *Page {
	return &Page{
		PageNo:   pageNo,
		dirty:    false,
		pageData: data,
	}
}

// @description: change *page to bytes
func (page *Page) Encode() []byte {
	page.lock.Lock()
	defer page.lock.Unlock()
	buf := bytes.NewBuffer(make([]byte, util.PageSize))
	binary.Write(buf, binary.BigEndian, page.pageType)
	binary.Write(buf, binary.BigEndian, page.PageNo)
	binary.Write(buf, binary.BigEndian, page.prevPageNo)
	binary.Write(buf, binary.BigEndian, page.nextPageNo)
	binary.Write(buf, binary.BigEndian, page.dirty)
	binary.Write(buf, binary.BigEndian, page.LSN)
	// page data needs special encode for different data types
	dataBytes := page.pageData.Encode()
	buf.Write(dataBytes)
	// fill the page with 0s if the page content is smaller than a page
	zeroLen := util.PageSize - len(buf.Bytes())
	buf.Write(make([]byte, zeroLen))
	return buf.Bytes()
}

// @description: change bytes to *page
func (page *Page) Decode(r io.Reader, pageData PageData) error {
	page.lock.Lock()
	defer page.lock.Unlock()
	// reading order should be the order they are defined in the prev code
	err := binary.Read(r, binary.BigEndian, &page.pageType)
	if err != nil {
		log.Errorf("fail to get page type: %v", err)
		return err
	}
	err = binary.Read(r, binary.BigEndian, &page.PageNo)
	if err != nil {
		log.Errorf("fail to get page number: %v", err)
	}
	err = binary.Read(r, binary.BigEndian, &page.prevPageNo)
	if err != nil {
		log.Errorf("fail to get previous page number: %v", err)
	}

	err = binary.Read(r, binary.BigEndian, &page.nextPageNo)
	if err != nil {
		log.Errorf("fail to get next page number: %v", err)
	}
	err = binary.Read(r, binary.BigEndian, &page.dirty)
	if err != nil {
		log.Errorf("fail to get page status: %v", err)
	}
	err = binary.Read(r, binary.BigEndian, &page.LSN)
	if err != nil {
		log.Errorf("fail to get page LSN: %v", err)
	}
	page.pageData = pageData
	err = page.pageData.Decode(r)
	if err != nil {
		log.Errorf("fail to get page data: %v", err)
	}
	return err
}

// @return: how many bytes a page has used
func (page *Page) Size() int {
	// pageType + pageNo + prevePageNo + nextPageNo + dirty + pageData
	// uint8 + uint32 + uint32 + uint32 + bool + int64 + pageData
	return 1 + 4 + 4 + 4 + 1 + 8 + page.pageData.Size()
}

// @description: get page data
func (page *Page) GetPageData() PageData {
	return page.pageData
}

func (page *Page) GetNextPageNo() uint32 {
	return page.nextPageNo
}

func (page *Page) GetPrevPageNo() uint32 {
	return page.prevPageNo
}
