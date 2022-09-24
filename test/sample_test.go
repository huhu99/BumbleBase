package test

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	btree "github.com/brown-csci1270/db/pkg/btree"
	concurrency "github.com/brown-csci1270/db/pkg/concurrency"
	db "github.com/brown-csci1270/db/pkg/db"
	hash "github.com/brown-csci1270/db/pkg/hash"
)

var BUFFER_SIZE int = 1024
var DELAY_TIME = 5 * time.Millisecond
var MAX_DELAY int64 = 10

func TestConcurrencyTA(t *testing.T) {
	t.Run("TestTransactionBasic", testTransactionBasic)
	t.Run("TestTransactionDeadlock", testTransactionDeadlock)
	//t.Run("TestDeadlockSimple", testDeadlockSimple)
	//t.Run("TestDeadlockDAG", testDeadlockDAG)
	//t.Run("TestConcurrentHashInsert", testConcurrentHashInsert)
	//t.Run("TestConcurrentBTreeInsert", testConcurrentBTreeInsert)
	//t.Run("TestConcurrentBTreeInsertRandom", testConcurrentBTreeInsertRandom)
}

type LockData struct {
	done bool
	key  int64
	lock bool
	lt   concurrency.LockType
}

// =====================================================================
// HELPERS
// =====================================================================

func setupConcurrency(t *testing.T) (*concurrency.TransactionManager, db.Index, string) {
	tmpfile, err := ioutil.TempFile(".", "db-*")
	if err != nil {
		t.Error(err)
	}
	defer tmpfile.Close()
	table, err := btree.OpenTable(tmpfile.Name())
	if err != nil {
		t.Error(err)
	}
	lm := concurrency.NewLockManager()
	tm := concurrency.NewTransactionManager(lm)
	return tm, table, tmpfile.Name()
}

func getTransactionThread() (uuid.UUID, chan LockData) {
	tid := uuid.New()
	ch := make(chan LockData, BUFFER_SIZE)
	return tid, ch
}

func handleTransactionThread(tm *concurrency.TransactionManager, table db.Index, tid uuid.UUID, ch chan LockData, errch chan error) {
	var ld LockData
	var err error
	tm.Begin(tid)
	for {
		// Get next command
		ld = <-ch
		// Terminate if done
		if ld.done {
			break
		}
		// Lock or unlock
		if ld.lock {
			err = tm.Lock(tid, table, ld.key, ld.lt)
		} else {
			err = tm.Unlock(tid, table, ld.key, ld.lt)
		}
		// Terminate if error
		if err != nil {
			errch <- err
			break
		}
	}
	tm.Commit(tid)
}

func sendWithDelay(ch chan LockData, ld LockData) {
	time.Sleep(DELAY_TIME)
	ch <- ld
}

func checkNoErrors(t *testing.T, errch chan error) {
	time.Sleep(DELAY_TIME)
	select {
	case err, ok := <-errch:
		if ok {
			t.Error(err)
		}
	default:
		t.Log("no errors")
	}
}

func checkWasErrors(t *testing.T, errch chan error) {
	time.Sleep(DELAY_TIME)
	select {
	case err, ok := <-errch:
		if ok {
			t.Log(err)
		}
	default:
		t.Error("expected an error")
	}
}

// =====================================================================
// TESTS (Transactions)
// =====================================================================

func testTransactionBasic(t *testing.T) {
	// Setup
	tm, table, filename := setupConcurrency(t)
	defer table.Close()
	defer os.Remove(filename)
	errch := make(chan error, BUFFER_SIZE)
	// Set up transactions
	tid1, ch1 := getTransactionThread()
	go handleTransactionThread(tm, table, tid1, ch1, errch)
	// Sending instructions
	sendWithDelay(ch1, LockData{key: 0, lock: true, lt: concurrency.W_LOCK})
	sendWithDelay(ch1, LockData{done: true})
	// Check for errors
	checkNoErrors(t, errch)
}

func testTransactionDeadlock(t *testing.T) {
	// Setup
	tm, table, filename := setupConcurrency(t)
	defer table.Close()
	defer os.Remove(filename)
	errch := make(chan error, BUFFER_SIZE)
	// Set up transactions
	tid1, ch1 := getTransactionThread()
	go handleTransactionThread(tm, table, tid1, ch1, errch)
	tid2, ch2 := getTransactionThread()
	go handleTransactionThread(tm, table, tid2, ch2, errch)
	// Sending instructions
	sendWithDelay(ch1, LockData{key: 0, lock: true, lt: concurrency.W_LOCK})
	sendWithDelay(ch2, LockData{key: 1, lock: true, lt: concurrency.W_LOCK})
	sendWithDelay(ch1, LockData{key: 1, lock: true, lt: concurrency.W_LOCK})
	sendWithDelay(ch2, LockData{key: 0, lock: true, lt: concurrency.W_LOCK})
	sendWithDelay(ch1, LockData{done: true})
	sendWithDelay(ch2, LockData{done: true})
	// Check for errors
	checkWasErrors(t, errch)
}

// =====================================================================
// TESTS (Deadlock)
// =====================================================================

func testDeadlockSimple(t *testing.T) {
	t1 := concurrency.Transaction{}
	t2 := concurrency.Transaction{}
	g := concurrency.NewGraph()
	g.AddEdge(&t1, &t2)
	g.AddEdge(&t2, &t1)
	if !g.DetectCycle() {
		t.Error("failed to detect cycle")
	}
}

func testDeadlockDAG(t *testing.T) {
	t1 := concurrency.Transaction{}
	t2 := concurrency.Transaction{}
	t3 := concurrency.Transaction{}
	t4 := concurrency.Transaction{}
	g := concurrency.NewGraph()
	g.AddEdge(&t1, &t2)
	g.AddEdge(&t1, &t3)
	g.AddEdge(&t2, &t4)
	g.AddEdge(&t3, &t4)
	if g.DetectCycle() {
		t.Error("cycle detected in DAG")
	}
}

// =====================================================================
// TESTS (Fine-grain Locking)
// =====================================================================

// Mod vals by this value to prevent hardcoding tests
var hash_salt int64 = rand.Int63n(1000)

func getTempHashDB(t *testing.T) string {
	tmpfile, err := ioutil.TempFile(".", "db-*")
	if err != nil {
		t.Error(err)
	}
	defer tmpfile.Close()
	return tmpfile.Name()
}

func getTempBTreeDB(t *testing.T) string {
	tmpfile, err := ioutil.TempFile(".", "db-*")
	if err != nil {
		t.Error(err)
	}
	defer tmpfile.Close()
	return tmpfile.Name()
}

func jitter() time.Duration {
	return time.Duration(rand.Int63n(MAX_DELAY)+1) * time.Millisecond
}

func insertKeys(t *testing.T, table db.Index, c chan int64, done chan bool) {
	for v := range c {
		time.Sleep(jitter())
		err := table.Insert(v, v%hash_salt)
		if err != nil {
			t.Error("Concurrent insert failed")
		}
	}
	done <- true
}

func deleteKeys(t *testing.T, table db.Index, c chan int64, done chan bool) {
	for v := range c {
		time.Sleep(jitter())
		err := table.Delete(v)
		if err != nil {
			t.Error("Concurrent insert failed")
		}
	}
	done <- true
}

func testConcurrentHashInsert(t *testing.T) {
	dbName := getTempHashDB(t)
	defer os.Remove(dbName)
	defer os.Remove(dbName + ".meta")
	// Init the database
	index, err := hash.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Queue entries for insertion
	nums := make(chan int64, 100)
	inserted := make([]int64, 0)
	target := int64(3)
	targetDepth := int64(4)
	go func() {
		cur := int64(0)
		for i := int64(0); i <= 5000; i++ {
			for {
				cur += 1
				if hash.Hasher(cur, targetDepth) == target {
					nums <- cur
					inserted = append(inserted, cur)
					break
				}
			}
		}
		close(nums)
	}()
	done := make(chan bool)
	numThreads := 4
	for i := 0; i < numThreads; i++ {
		go insertKeys(t, index, nums, done)
	}
	for i := 0; i < numThreads; i++ {
		<-done
	}
	// Retrieve entries
	for _, i := range inserted {
		entry, err := index.Find(i)
		if err != nil {
			t.Error(err)
		}
		if entry == nil {
			t.Error("Inserted entry could not be found")
		}
		if entry.GetKey() != i {
			t.Error("Entry with wrong entry was found")
		}
		if entry.GetValue() != i%hash_salt {
			t.Error("Entry found has the wrong value")
		}
	}
	index.Close()
}

func testConcurrentBTreeInsert(t *testing.T) {
	dbName := getTempBTreeDB(t)
	defer os.Remove(dbName)
	// Init the database
	index, err := btree.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Queue entries for insertion
	nums := make(chan int64, 100)
	inserted := make([]int64, 0)
	go func() {
		for i := int64(0); i <= 5000; i++ {
			nums <- i
			inserted = append(inserted, i)
		}
		close(nums)
	}()
	done := make(chan bool)
	numThreads := 4
	for i := 0; i < numThreads; i++ {
		go insertKeys(t, index, nums, done)
	}
	for i := 0; i < numThreads; i++ {
		<-done
	}
	// Retrieve entries
	for _, i := range inserted {
		entry, err := index.Find(i)
		if err != nil {
			t.Error(err)
		}
		if entry == nil {
			t.Error("Inserted entry could not be found")
		}
		if entry.GetKey() != i {
			t.Error("Entry with wrong entry was found")
		}
		if entry.GetValue() != i%hash_salt {
			t.Error("Entry found has the wrong value")
		}
	}
	index.Close()
}

func testConcurrentBTreeInsertRandom(t *testing.T) {
	dbName := getTempBTreeDB(t)
	defer os.Remove(dbName)
	// Init the database
	index, err := btree.OpenTable(dbName)
	if err != nil {
		t.Error(err)
	}
	// Queue entries for insertion
	nums := make(chan int64, 100)
	inserted := make([]int64, 0)
	go func() {
		for i := int64(0); i <= 5000; i++ {
			rand.Seed(i)
			r := rand.Intn(100000)
			nums <- int64(r)
			inserted = append(inserted, int64(r))
		}
		close(nums)
	}()
	done := make(chan bool)
	numThreads := 4
	for i := 0; i < numThreads; i++ {
		go insertKeys(t, index, nums, done)
	}
	for i := 0; i < numThreads; i++ {
		<-done
	}
	// Retrieve entries
	for _, i := range inserted {
		entry, err := index.Find(i)
		if err != nil {
			t.Error(err)
		}
		if entry == nil {
			t.Error("Inserted entry could not be found")
		}
		if entry.GetKey() != i {
			t.Error("Entry with wrong entry was found")
		}
		if entry.GetValue() != i%hash_salt {
			t.Error("Entry found has the wrong value")
		}
	}
	index.Close()
}