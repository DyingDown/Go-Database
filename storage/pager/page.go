package pager

import (
	"bytes"
	"encoding/binary"
	"go-database/util"
	"io"

	log "github.com/sirupsen/logrus"
)

/*
 *  page has three types
 *	mata page:  meta information of table
 * 	data page:  data of table
 * 	index page: index(bplustree node) information
 */

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
	pageType   PageType
	PageNo     uint32 // page number
	prevPageNo uint32
	nextPageNo uint32
	dirty      bool
	pageData   PageData // content of page
}

func NewPage(pageNo uint32, data PageData) *Page {
	return &Page{
		pageType: ptype,
		PageNo:   pageNo,
		dirty:    false,
		pageData: data,
	}
}

// change *page to bytes
func (page *Page) Encode() []byte {
	buf := bytes.NewBuffer(make([]byte, util.PageSize))
	binary.Write(buf, binary.BigEndian, page.pageType)
	binary.Write(buf, binary.BigEndian, page.pageNo)
	binary.Write(buf, binary.BigEndian, page.prevPageNo)
	binary.Write(buf, binary.BigEndian, page.nextPageNo)
	// page data needs special encode for different data types
	dataBytes := page.pageData.Encode()
	buf.Write(dataBytes)
	// fill the page with 0s if the page content is smaller than a page
	zeroLen := util.PageSize - len(buf.Bytes())
	buf.Write(make([]byte, zeroLen))
	return buf.Bytes()
}

func (page *Page) Decode(r io.Reader, pageData PageData) error {
	err := binary.Read(r, binary.BigEndian, &page.pageType)
	if err != nil {
		log.Errorf("fail to get page type: %v", err)
		return err
	}
	err = binary.Read(r, binary.BigEndian, &page.pageNo)
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
	page.pageData = pageData
	err = page.pageData.Decode(r)
	if err != nil {
		log.Errorf("fail to get page data: %v", err)
	}
	return err
}

func (page *Page) Size() int {
	return 1 + 4 + 4 + 4 + 1 + page.pageData.Size()
}
