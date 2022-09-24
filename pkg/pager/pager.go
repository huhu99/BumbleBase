package pager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	config "github.com/brown-csci1270/db/pkg/config"
	list "github.com/brown-csci1270/db/pkg/list"

	directio "github.com/ncw/directio"
)

// Page size - defaults to 4kb.
const PAGESIZE = int64(directio.BlockSize)

// Number of pages.
const NUMPAGES = config.NumPages

// Pagers manage pages of data read from a file.
type Pager struct {
	file         *os.File             // File descriptor.
	nPages       int64                // The number of pages used by this database.
	ptMtx        sync.Mutex           // Page table mutex.
	freeList     *list.List           // Free page list.
	unpinnedList *list.List           // Unpinned page list.
	pinnedList   *list.List           // Pinned page list.
	pageTable    map[int64]*list.Link // Page table.
}

// Construct a new Pager.
func NewPager() *Pager {
	var pager *Pager = &Pager{}
	pager.pageTable = make(map[int64]*list.Link)
	pager.freeList = list.NewList()
	pager.unpinnedList = list.NewList()
	pager.pinnedList = list.NewList()
	frames := directio.AlignedBlock(int(PAGESIZE * NUMPAGES))
	for i := 0; i < NUMPAGES; i++ {
		frame := frames[i*int(PAGESIZE) : (i+1)*int(PAGESIZE)]
		page := Page{
			pager:    pager,
			pagenum:  NOPAGE,
			pinCount: 0,
			dirty:    false,
			data:     &frame,
		}
		pager.freeList.PushTail(&page)
	}
	return pager
}

// HasFile checks if the pager is backed by disk.
func (pager *Pager) HasFile() bool {
	return pager.file != nil
}

// TODO: Replace the following function
// GetFileName returns the file name.
func (pager *Pager) GetFileName() string {
	return filepath.Base(pager.file.Name())
}

// GetNumPages returns the number of pages.
func (pager *Pager) GetNumPages() int64 {
	return pager.nPages
}

// GetFreePN returns the next available page number.
func (pager *Pager) GetFreePN() int64 {
	// Assign the first page number beyond the end of the file.
	return pager.nPages
}

// Open initializes our page with a given database file.
func (pager *Pager) Open(filename string) (err error) {
	// Create the necessary prerequisite directories.
	if idx := strings.LastIndex(filename, "/"); idx != -1 {
		err = os.MkdirAll(filename[:idx], 0775)
		if err != nil {
			return err
		}
	}
	// Open or create the db file.
	pager.file, err = directio.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	// Get info about the size of the pager.
	var info os.FileInfo
	var len int64
	if info, err = pager.file.Stat(); err == nil {
		len = info.Size()
		if len%PAGESIZE != 0 {
			return errors.New("open: DB file has been corrupted")
		}
	}
	// Set the number of pages and hand off initialization to someone else.
	pager.nPages = len / PAGESIZE
	return nil
}

// Close signals our pager to flush all dirty pages to disk.
func (pager *Pager) Close() (err error) {
	// Prevent new data from being paged in.
	pager.ptMtx.Lock()
	// Check if all refcounts are 0.
	curLink := pager.pinnedList.PeekHead()
	if curLink != nil {
		fmt.Println("ERROR: pages are still pinned on close")
	}
	// Cleanup.
	pager.FlushAllPages()
	if pager.file != nil {
		err = pager.file.Close()
	}
	pager.ptMtx.Unlock()
	return err
}

// Populate a page's data field, given a pagenumber.
func (pager *Pager) ReadPageFromDisk(page *Page, pagenum int64) error {
	if _, err := pager.file.Seek(pagenum*PAGESIZE, 0); err != nil {
		return err
	}
	if _, err := pager.file.Read(*page.data); err != nil && err != io.EOF {
		return err
	}
	return nil
}

// NewPage returns an unused buffer from the free or unpinned list
// the ptMtx should be locked on entry
func (pager *Pager) NewPage(pagenum int64) (*Page, error) {
	/* SOLUTION {{{ */
	var newPage *Page
	if freeLink := pager.freeList.PeekHead(); freeLink != nil {
		// Check the free list first
		freeLink.PopSelf()
		newPage = freeLink.GetKey().(*Page)
	} else if unpinLink := pager.unpinnedList.PeekHead(); pager.HasFile() && unpinLink != nil {
		// If no page was found, evict a page from the unpinned list.
		// But skip this if our pager isn't backed by disk.
		unpinLink.PopSelf()
		newPage = unpinLink.GetKey().(*Page)
		pager.FlushPage(newPage)
		delete(pager.pageTable, newPage.pagenum)
	} else {
		// If still no page is found, error.
		return nil, errors.New("no available pages")
	}
	newPage.pagenum = pagenum
	newPage.dirty = false
	newPage.pinCount = 1
	return newPage, nil
	/* SOLUTION }}} */
}

// getPage returns the page corresponding to the given pagenum.
func (pager *Pager) GetPage(pagenum int64) (page *Page, err error) {
	/* SOLUTION {{{ */
	// Input checking.
	if pagenum < 0 {
		return nil, errors.New("invalid pagenum")
	}
	// Try to get from page table.
	var newLink *list.Link
	pager.ptMtx.Lock()
	defer pager.ptMtx.Unlock()
	link, ok := pager.pageTable[pagenum]
	if ok {
		page = link.GetKey().(*Page)
		// Move the page to the pinned list if needed.
		if link.GetList() == pager.unpinnedList {
			link.PopSelf()
			newLink = pager.pinnedList.PushTail(page)
			pager.pageTable[pagenum] = newLink
		}
		page.Get()
		return page, nil
	}
	// Else, create a buffer to hold the new page in.
	page, err = pager.NewPage(pagenum)
	if err != nil {
		return nil, err
	}

	// Check if we need to create a new page.
	if pagenum >= pager.nPages {
		pager.nPages++
		page.dirty = true
	} else {
		// Read an existing page in.
		page.dirty = false
		err = pager.ReadPageFromDisk(page, pagenum)
		if err != nil {
			pager.freeList.PushTail(page)
			return nil, err
		}
	}
	// Insert the page into our list of pages.
	newLink = pager.pinnedList.PushTail(page)
	pager.pageTable[pagenum] = newLink
	return page, nil
	/* SOLUTION }}} */
}

// Flush a particular page to disk.
func (pager *Pager) FlushPage(page *Page) {
	/* SOLUTION {{{ */
	if pager.HasFile() && page.IsDirty() {
		pager.file.WriteAt(
			*page.data,
			page.pagenum*PAGESIZE,
		)
		page.SetDirty(false)
	}
	/* SOLUTION }}} */
}

// Flushes all dirty pages.
func (pager *Pager) FlushAllPages() {
	/* SOLUTION {{{ */
	writer := func(link *list.Link) {
		page := link.GetKey().(*Page)
		pager.FlushPage(page)
	}
	pager.pinnedList.Map(writer)
	pager.unpinnedList.Map(writer)
	/* SOLUTION }}} */
}

// [RECOVERY] Block all updates.
func (pager *Pager) LockAllUpdates() {
	pager.ptMtx.Lock()
	for _, page := range pager.pageTable {
		page.GetKey().(*Page).LockUpdates()
	}
}

// [RECOVERY] Enable updates.
func (pager *Pager) UnlockAllUpdates() {
	for _, page := range pager.pageTable {
		page.GetKey().(*Page).UnlockUpdates()
	}
	pager.ptMtx.Unlock()
}
