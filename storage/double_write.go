/*
 *	Double write ensures a page's integrity, but dose not ensure that the page is the newest.
 *	1. copy the modified page to DoubleWrite buffer in memory
 *	2. when the buffer is full, or when the database exit normally
 *		2.1 copy the changed pages, and then set the buffer empty
 *		2.2 write the changed pages to disk buffer
 *		2.3 write the changed pages to actural database file
 *		2.4 update check point value : select the biggest LSN from pages as the new check point
 *	3. when the database exit abnormally
 *		3.1 read the changed pages from disk buffer
 *		3.2 compute the check sum of each page in buffer file(check sum stores in each page's last four bytes)
 * 			3.2.1 if the check sum does not match, then break and clear the buffer
 *          3.2.2 then let redo log to recover the lost pages
 */
package doublewrite

import (
	"bytes"
	"go-database/storage/pager"
	"go-database/util"
	"log"
	"os"
	"sync"
)

var NULL_Buffer []byte = make([]byte, util.PageSize*util.DoubleWriteBufferSize)

type DoubleWrite struct {
	PageBuffer *os.File          // buffer file
	PageFile   *os.File          // database file
	Pages      map[uint32][]byte // page in memory
	CheckPoint int64
	memoryLock sync.Mutex
	diskLock   sync.Mutex
}

func Open(path string, pagefile *os.File) *DoubleWrite {
	buffer, err := os.OpenFile(path+"_buffer", os.O_RDWR, 0666)
	if err != nil {
		panic("fail open file " + path + "_buffer")
	}
	return &DoubleWrite{
		PageBuffer: buffer,
		PageFile:   pagefile,
		Pages:      make(map[uint32][]byte),
	}
}

func Create(path string, pagefile *os.File) *DoubleWrite {
	// if file already exists
	if status, err := os.Stat(path + "_buffer"); err == nil && status.Size() != 0 {
		log.Fatal("DoubleWrite buffer file already exists")
		return Open(path, pagefile)
	}
	buffer, err := os.OpenFile(path+"_buffer", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic("fail create file " + path + "_buffer")
	}
	buffer.Write(NULL_Buffer)
	return &DoubleWrite{
		PageBuffer: buffer,
		PageFile:   pagefile,
		Pages:      make(map[uint32][]byte),
	}
}

// @description: add modified page to memory buffer
func (dw *DoubleWrite) Write(page *pager.Page) {
	pageBytes := page.Encode()

	dw.memoryLock.Lock()
	defer dw.memoryLock.Unlock()

	// calculate the check sum
	checkSum := util.CheckSum(pageBytes)
	// write the check sum to page
	copy(pageBytes[len(pageBytes)-4:], checkSum)
	// store page in memory buffer
	dw.Pages[page.PageNo] = pageBytes

	// if the buffer is full, then write the buffer to disk
	if len(dw.Pages) >= util.DoubleWriteBufferSize {
		go dw.Flush()
	}
}

// @description: flush pages in memory to disk buffer file
func (dw *DoubleWrite) Flush() {
	dw.memoryLock.Lock()
	// copy the pages
	pages := dw.Pages
	// clear the dw.Pages
	dw.Pages = make(map[uint32][]byte)
	dw.memoryLock.Unlock()

	// splice the pages
	// also find the biggest LSN as the check point
	var splicedPages []byte
	var maxLSN int64 = 0
	for k, v := range pages {
		splicedPages = append(splicedPages, v...)
		if int64(k) > maxLSN {
			maxLSN = int64(k)
		}
	}

	// write the spliced pages to disk buffer file
	_, err := dw.PageBuffer.Write(splicedPages)
	if err != nil {
		log.Fatal("fail to write to double write buffer")
	}

	// write changed pages to database file
	dw.diskLock.Lock()
	defer dw.diskLock.Unlock()
	for k, v := range pages {
		_, err := dw.PageFile.Seek(int64(util.PageSize*k), 0)
		if err != nil {
			log.Fatal("fail to seek to page " + string(k))
		}
		_, err = dw.PageFile.Write(v)
		if err != nil {
			log.Fatal("fail to write to page " + string(k))
		}
		lsn := util.LSN(v)
		if lsn > maxLSN {
			maxLSN = lsn
		}
	}

	// empty the disk buffer file
	dw.PageBuffer.Seek(0, 0)
	dw.PageBuffer.Write(NULL_Buffer)

	// TODO: update check point
}

func (dw *DoubleWrite) Close() {
	dw.Flush()
	dw.PageBuffer.Close()
}

// @description: check if the pages crashed and recover the crashed page
func (dw *DoubleWrite) Recover() {
	dw.PageBuffer.Seek(0, 0)
	page := make([]byte, util.PageSize)
	for size, err := dw.PageBuffer.Read(page); size == util.PageSize && err == nil; {
		// if page is empty then the page is valid
		if bytes.Equal(page, make([]byte, util.PageSize)) {
			// pages after empty page are also empty
			break
		}

		// if page is not empty, then check the check sum
		// check if the page is crashed
		if !bytes.Equal(util.CheckSum(page), util.CheckSum(page[:len(page)-4])) {
			// if the page is not crashed
			break
		}
		pageNo := util.BytesToUInt32(page[1:5])
		dw.PageFile.Seek(int64(pageNo*util.PageSize), 0)
		dw.PageFile.Write(page)
	}
}
