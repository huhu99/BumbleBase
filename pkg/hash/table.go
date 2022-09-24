package hash

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	pager "github.com/brown-csci1270/db/pkg/pager"
	utils "github.com/brown-csci1270/db/pkg/utils"
)

// HashTable definitions.
type HashTable struct {
	depth   int64
	buckets []int64 // Array of bucket page numbers
	pager   *pager.Pager
	rwlock  sync.RWMutex // Lock on the hash table index
}

// Returns a new HashTable.
func NewHashTable(pager *pager.Pager) (*HashTable, error) {
	depth := int64(2)
	buckets := make([]int64, powInt(2, depth))
	for i := range buckets {
		bucket, err := NewHashBucket(pager, depth)
		if err != nil {
			return nil, err
		}
		buckets[i] = bucket.page.GetPageNum()
		bucket.page.Put()
	}
	return &HashTable{depth: depth, buckets: buckets, pager: pager}, nil
}

// [CONCURRENCY] Grab a write lock on the hash table index
func (table *HashTable) WLock() {
	table.rwlock.Lock()
}

// [CONCURRENCY] Release a write lock on the hash table index
func (table *HashTable) WUnlock() {
	table.rwlock.Unlock()
}

// [CONCURRENCY] Grab a read lock on the hash table index
func (table *HashTable) RLock() {
	table.rwlock.RLock()
}

// [CONCURRENCY] Release a read lock on the hash table index
func (table *HashTable) RUnlock() {
	table.rwlock.RUnlock()
}

// Get depth.
func (table *HashTable) GetDepth() int64 {
	return table.depth
}

// Get bucket page numbers.
func (table *HashTable) GetBuckets() []int64 {
	return table.buckets
}

// Get pager.
func (table *HashTable) GetPager() *pager.Pager {
	return table.pager
}

// Finds the entry with the given key.
func (table *HashTable) Find(key int64) (utils.Entry, error) {
	/* SOLUTION {{{ */
	// Hash the key.
	table.RLock()
	defer table.RUnlock()
	hash := Hasher(key, table.depth)
	if hash < 0 || int(hash) >= len(table.buckets) {
		return nil, errors.New("not found")
	}
	// Get the corresponding bucket.
	bucket, err := table.GetBucket(hash, READ_LOCK)
	defer bucket.RUnlock()
	if err != nil {
		return nil, err
	}
	defer bucket.page.Put()

	// Find the entry.
	entry, found := bucket.Find(key)
	if !found {
		return nil, errors.New("not found")
	}
	return entry, nil
	/* SOLUTION }}} */
}

// ExtendTable increases the global depth of the table by 1.
func (table *HashTable) ExtendTable() {
	table.depth = table.depth + 1
	table.buckets = append(table.buckets, table.buckets...)
}

// Split the given bucket into two, extending the table if necessary.
func (table *HashTable) Split(bucket *HashBucket, hash int64) error {
	/* SOLUTION {{{ */
	// Figure out where the new pointer should live.
	oldHash := (hash % powInt(2, bucket.depth))
	newHash := oldHash + powInt(2, bucket.depth)
	// If we are splitting, check if we need to double the table first.
	if bucket.depth == table.depth {
		table.ExtendTable()
	}
	// Next, make a new bucket.
	bucket.updateDepth(bucket.depth + 1)
	newBucket, err := NewHashBucket(table.pager, bucket.depth)
	if err != nil {
		return err
	}
	defer newBucket.page.Put()

	// Move entries over to it.
	tmpEntries := make([]HashEntry, bucket.numKeys)
	for i := int64(0); i < bucket.numKeys; i++ {
		tmpEntries[i] = bucket.getCell(i)
	}
	oldNKeys := int64(0)
	newNKeys := int64(0)
	for _, entry := range tmpEntries {
		if Hasher(entry.GetKey(), bucket.depth) == newHash {
			newBucket.modifyCell(newNKeys, entry)
			newNKeys++
		} else {
			bucket.modifyCell(oldNKeys, entry)
			oldNKeys++
		}
	}
	// Initialize bucket attributes.
	bucket.updateNumKeys(oldNKeys)
	newBucket.updateNumKeys(newNKeys)
	power := bucket.depth
	// Point the rest of the buckets to the new page.
	for i := newHash; i < powInt(2, table.depth); {
		table.buckets[i] = newBucket.page.GetPageNum()
		i += powInt(2, power)
	}
	// Check if recursive splitting is required
	if oldNKeys >= BUCKETSIZE {
		return table.Split(bucket, oldHash)
	}
	if newNKeys >= BUCKETSIZE {
		return table.Split(newBucket, newHash)
	}
	return nil
	/* SOLUTION }}} */
}

// Inserts the given key-value pair, splits if necessary.
func (table *HashTable) Insert(key int64, value int64) error {
	table.WLock()
	defer table.WUnlock()
	/* SOLUTION {{{ */
	hash := Hasher(key, table.depth)
	bucket, err := table.GetBucket(hash, WRITE_LOCK)
	defer bucket.WUnlock()
	if err != nil {
		return err
	}
	defer bucket.page.Put()

	split, err := bucket.Insert(key, value)
	if err != nil {
		return err
	}
	if !split {
		return nil
	}

	return table.Split(bucket, hash)
	/* SOLUTION }}} */
}

// Update the given key-value pair.
func (table *HashTable) Update(key int64, value int64) error {
	table.WLock()
	defer table.WUnlock()
	/* SOLUTION {{{ */
	hash := Hasher(key, table.depth)
	bucket, err := table.GetBucket(hash, WRITE_LOCK)
	defer bucket.WUnlock()
	if err != nil {
		return err
	}
	defer bucket.page.Put()
	return bucket.Update(key, value)
	/* SOLUTION }}} */
}

// Delete the given key-value pair, does not coalesce.
func (table *HashTable) Delete(key int64) error {
	table.WLock()
	defer table.WUnlock()
	/* SOLUTION {{{ */
	hash := Hasher(key, table.depth)
	bucket, err := table.GetBucket(hash, WRITE_LOCK)
	if err != nil {
		return err
	}
	defer bucket.page.Put()
	defer bucket.WUnlock()
	return bucket.Delete(key)
	/* SOLUTION }}} */
}

// Select all entries in this table.
func (table *HashTable) Select() ([]utils.Entry, error) {
	table.RLock()
	defer table.RUnlock()
	/* SOLUTION {{{ */
	ret := make([]utils.Entry, 0)
	for i := int64(0); i < table.pager.GetNumPages(); i++ {
		bucket, err := table.GetBucketByPN(i, READ_LOCK)
		defer bucket.RUnlock()
		if err != nil {
			return nil, err
		}
		entries, err := bucket.Select()
		bucket.GetPage().Put()
		if err != nil {
			return nil, err
		}
		ret = append(ret, entries...)
	}
	return ret, nil
	/* SOLUTION }}} */
}

// Print out each bucket.
func (table *HashTable) Print(w io.Writer) {
	table.RLock()
	defer table.RUnlock()
	io.WriteString(w, "====\n")
	io.WriteString(w, fmt.Sprintf("global depth: %d\n", table.depth))
	for i := range table.buckets {
		io.WriteString(w, fmt.Sprintf("====\nbucket %d\n", i))
		bucket, err := table.GetBucket(int64(i), READ_LOCK)
		if err != nil {
			continue
		}
		bucket.RLock()
		bucket.Print(w)
		bucket.RUnlock()
		bucket.page.Put()
	}
	io.WriteString(w, "====\n")
}

// Print out a specific bucket.
func (table *HashTable) PrintPN(pn int, w io.Writer) {
	table.RLock()
	defer table.RUnlock()
	if int64(pn) >= table.pager.GetNumPages() {
		fmt.Println("out of bounds")
		return
	}
	bucket, err := table.GetBucketByPN(int64(pn), READ_LOCK)
	if err != nil {
		return
	}
	bucket.RLock()
	bucket.Print(w)
	bucket.RUnlock()
	bucket.page.Put()
}

// x^y
func powInt(x, y int64) int64 {
	return int64(math.Pow(float64(x), float64(y)))
}
