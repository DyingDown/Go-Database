/*
 *	Pager:
 * 		cache:   used to store frequently visited page to increase speed of loading pages
 *		os.File: sql file
 *
 *	Main Functioins:
 *		Get data from page
 *		Write data into page
 *		Find the right page to store data
 */

package pager

import (
	"fmt"
	"go-database/storage/pager/pagedata"
	"go-database/util"
	"go-database/util/cache"
	"math"
	"os"

	log "github.com/sirupsen/logrus"
)

type Pager struct {
	cache *cache.Cache
	file  *os.File
}

// sql file already existed
func OpenFile(filepath string) *Pager {
	c := cache.CreateCache()
	f, err := os.OpenFile(filepath, os.O_RDWR, 0666)
	if err != nil {
		panic("fail open file " + filepath)
	}
	pager := &Pager{
		cache: c,
		file:  f,
	}
	metaPage, err := pager.GetPage(0, pagedata.NewMetaData())
	if err != nil {
		log.Fatalf("fail to open file %v: %v", filepath, err)
	}
	// add meta page to cache
	pager.cache.AddData(0, metaPage)
	return pager
}

// @description: create a new sql file
func CreateFile(filepath string) *Pager {
	c := cache.CreateCache()
	f, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic("fail create file " + filepath)
	}
	pager := &Pager{
		cache: c,
		file:  f,
	}
	metaData := pagedata.NewMetaData()
	metaPage := pager.CreatePage(metaData)
	pager.cache.AddData(0, metaPage)
	return pager
}

// @param: pageNum uint32    No. of page
// @param: pdata   PageData  An empty struct, as a container to receive data
// @description: load a page from cache or file
func (pager *Pager) GetPage(pageNum uint32, pdata PageData) (*Page, error) {
	data := pager.cache.GetData(pageNum)
	if data != nil {
		return data.(*Page), nil
	} else { // if the page is not in cache
		newPage := new(Page)
		err := newPage.Decode(pager.file, pdata)
		if err != nil {
			log.Errorf("fail to decode page: %v", err)
			return nil, err
		}
		// add the page to cache
		pager.cache.AddData(pageNum, newPage)
		return newPage, nil
	}
}

//
func (pager *Pager) CreatePage(data PageData) *Page {
	fileInfo, err := pager.file.Stat()
	if err != nil {
		log.Errorf("Fail to fetch file status: %v", err)
	}
	fileSize := fileInfo.Size()
	// PageNo*PageSize of offset relative to head of file
	newPageNo := uint32(fileSize / util.PageSize)
	page := NewPage(newPageNo, data)
	pager.WritePage(page)
	return page
}

// @description: select a usable page
// select the last page of the table
// if the last page's free space is not enough to store the new data, then create a new page
func (pager *Pager) SelectPage(dataSize int, tableName string) (page *Page, err error) {
	if dataSize > util.PageSize {
		return nil, fmt.Errorf("size of data is over a page's size")
	}
	metadata := pager.GetMetaData()
	table := metadata.GetTableInfo(tableName)
	page, err = pager.GetPage(table.LastPage, pagedata.NewRecordData())
	if err != nil {
		return nil, err
	}
	if util.PageSize-page.Size() < dataSize {
		// rest space of last page is not enough for new data
		// need to create a new page to store it
		newDataPage := pager.CreatePage(pagedata.NewRecordData())
		newDataPage.nextPageNo = math.MaxUint32
		newDataPage.prevPageNo = page.PageNo
		page.nextPageNo = newDataPage.PageNo
		table.LastPage = newDataPage.PageNo
		return newDataPage, nil
	} else {
		return page, nil
	}
}

// @description: flush page to disk
func (pager *Pager) WritePage(page *Page) {
	bytes := page.Encode()
	n, err := pager.file.WriteAt(bytes, int64(page.PageNo*util.PageSize))
	if err != nil || n != util.PageSize {
		panic("fail to write page to disk")
	}
	pager.file.Sync()
}

func (pager *Pager) Close() {
	pager.file.Close()
}

func (pager *Pager) GetMetaData() *pagedata.MetaData {
	metapage, err := pager.GetPage(0, pagedata.NewMetaData())
	if err != nil {
		log.Fatalf("fail to load meta page: %v", err)
	}
	return metapage.pageData.(*pagedata.MetaData)
}
