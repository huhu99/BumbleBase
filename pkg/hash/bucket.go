package hash

import (
	"errors"
	"fmt"
	"io"

	pager "github.com/brown-csci1270/db/pkg/pager"
	utils "github.com/brown-csci1270/db/pkg/utils"
)

// HashBucket.
type HashBucket struct {
	depth   int64
	numKeys int64
	page    *pager.Page
}

// Construct a new HashBucket.
func NewHashBucket(pager *pager.Pager, depth int64) (*HashBucket, error) {
	newPN := pager.GetFreePN()
	newPage, err := pager.GetPage(newPN)
	if err != nil {
		return nil, err
	}
	bucket := &HashBucket{depth: depth, numKeys: 0, page: newPage}
	bucket.updateDepth(depth)
	return bucket, nil
}

// Get local depth.
func (bucket *HashBucket) GetDepth() int64 {
	return bucket.depth
}

// Get a bucket's page.
func (bucket *HashBucket) GetPage() *pager.Page {
	return bucket.page
}

// Finds the entry with the given key.
func (bucket *HashBucket) Find(key int64) (utils.Entry, bool) {
	/* SOLUTION {{{ */
	for i := int64(0); i < bucket.numKeys; i++ {
		if bucket.getKeyAt(i) == key {
			return bucket.getCell(i), true
		}
	}
	return nil, false
	/* SOLUTION }}} */
}

// Inserts the given key-value pair, splits if necessary.
func (bucket *HashBucket) Insert(key int64, value int64) (bool, error) {
	/* SOLUTION {{{ */
	bucket.modifyCell(bucket.numKeys, HashEntry{key: key, value: value})
	bucket.updateNumKeys(bucket.numKeys + 1)
	return bucket.numKeys >= BUCKETSIZE, nil
	/* SOLUTION }}} */
}

// Update the given key-value pair, should never split.
func (bucket *HashBucket) Update(key int64, value int64) error {
	/* SOLUTION {{{ */
	// Get the index to update.
	index := int64(-1)
	for i := int64(0); i < bucket.numKeys; i++ {
		if bucket.getKeyAt(i) == key {
			index = i
			break
		}
	}
	if index == -1 {
		return errors.New("key not found, update aborted")
	}
	// Update the value.
	bucket.updateValueAt(index, value)
	return nil
	/* SOLUTION }}} */
}

// Delete the given key-value pair, does not coalesce.
func (bucket *HashBucket) Delete(key int64) error {
	/* SOLUTION {{{ */
	// Get the index to delete.
	index := int64(-1)
	for i := int64(0); i < bucket.numKeys; i++ {
		if bucket.getKeyAt(i) == key {
			index = i
			break
		}
	}
	if index == -1 {
		return errors.New("key not found, delete aborted")
	}
	// Move all other keys left by one.
	for i := index; i < bucket.numKeys; i++ {
		bucket.modifyCell(i, bucket.getCell(i+1))
	}
	bucket.updateNumKeys(bucket.numKeys - 1)
	return nil
	/* SOLUTION }}} */
}

// Select all entries in this bucket.
func (bucket *HashBucket) Select() ([]utils.Entry, error) {
	/* SOLUTION {{{ */
	ret := make([]utils.Entry, 0)
	for i := int64(0); i < bucket.numKeys; i++ {
		ret = append(ret, bucket.getCell(i))
	}
	return ret, nil
	/* SOLUTION }}} */
}

// Pretty-print this bucket.
func (bucket *HashBucket) Print(w io.Writer) {
	io.WriteString(w, fmt.Sprintf("bucket depth: %d\n", bucket.depth))
	io.WriteString(w, "entries:")
	for i := int64(0); i < bucket.numKeys; i++ {
		bucket.getCell(i).Print(w)
	}
	io.WriteString(w, "\n")
}

// [CONCURRENCY] Grab a write lock on the hash table index
func (bucket *HashBucket) WLock() {
	bucket.page.WLock()
}

// [CONCURRENCY] Release a write lock on the hash table index
func (bucket *HashBucket) WUnlock() {
	bucket.page.WUnlock()
}

// [CONCURRENCY] Grab a read lock on the hash table index
func (bucket *HashBucket) RLock() {
	bucket.page.RLock()
}

// [CONCURRENCY] Release a read lock on the hash table index
func (bucket *HashBucket) RUnlock() {
	bucket.page.RUnlock()
}
