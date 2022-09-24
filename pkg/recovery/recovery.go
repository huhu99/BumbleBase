package recovery

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	concurrency "github.com/brown-csci1270/db/pkg/concurrency"
	db "github.com/brown-csci1270/db/pkg/db"
	"github.com/otiai10/copy"

	uuid "github.com/google/uuid"
)

// Recovery Manager.
type RecoveryManager struct {
	d       *db.Database
	tm      *concurrency.TransactionManager
	txStack map[uuid.UUID]([]Log)
	fd      *os.File
	mtx     sync.Mutex
}

// Construct a recovery manager.
func NewRecoveryManager(
	d *db.Database,
	tm *concurrency.TransactionManager,
	logName string,
) (*RecoveryManager, error) {
	fd, err := os.OpenFile(logName, os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &RecoveryManager{
		d:       d,
		tm:      tm,
		txStack: make(map[uuid.UUID][]Log),
		fd:      fd,
	}, nil
}

// Write the string `s` to the log file. Expects rm.mtx to be locked
func (rm *RecoveryManager) writeToBuffer(s string) error {
	_, err := rm.fd.WriteString(s)
	if err != nil {
		return err
	}
	err = rm.fd.Sync()
	return err
}

// Write a Table log.
func (rm *RecoveryManager) Table(tblType string, tblName string) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	log := tableLog{tblType, tblName}
	rm.writeToBuffer(log.toString())
}

// Write an Edit log.
func (rm *RecoveryManager) Edit(clientId uuid.UUID, table db.Index, action Action, key int64, oldval int64, newval int64) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	log := editLog{clientId, table.GetName(), action, key, oldval, newval}
	rm.txStack[clientId] = append(rm.txStack[clientId], &log)
	rm.writeToBuffer(log.toString())
}

// Write a transaction start log.
func (rm *RecoveryManager) Start(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	log := startLog{clientId}
	rm.txStack[clientId] = append(rm.txStack[clientId], &log)
	rm.writeToBuffer(log.toString())
}

// Write a transaction commit log.
func (rm *RecoveryManager) Commit(clientId uuid.UUID) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	log := commitLog{clientId}
	delete(rm.txStack, clientId)
	rm.writeToBuffer(log.toString())
}

// Flush all pages to disk and write a checkpoint log.
func (rm *RecoveryManager) Checkpoint() {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()
	for _, tb := range rm.d.GetTables() {
		tb.GetPager().LockAllUpdates()
		tb.GetPager().FlushAllPages()
		defer tb.GetPager().UnlockAllUpdates()
	}
	activeTxs := make([]uuid.UUID, 0)
	for tx, _ := range rm.txStack {
		activeTxs = append(activeTxs, tx)
	}
	log := checkpointLog{activeTxs}
	rm.writeToBuffer(log.toString())
	rm.Delta() // Sorta-semi-pseudo-copy-on-write (to ensure db recoverability)
}

// Redo a given log's action.
func (rm *RecoveryManager) Redo(log Log) error {
	switch log := log.(type) {
	case *tableLog:
		payload := fmt.Sprintf("create %s table %s", log.tblType, log.tblName)
		err := db.HandleCreateTable(rm.d, payload, os.Stdout)
		if err != nil {
			return err
		}
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
			err := db.HandleInsert(rm.d, payload)
			if err != nil {
				// There is already an entry, try updating
				payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
				err = db.HandleUpdate(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.newval)
			err := db.HandleUpdate(rm.d, payload)
			if err != nil {
				// Entry may have been deleted, try inserting
				payload := fmt.Sprintf("insert %v %v into %s", log.key, log.newval, log.tablename)
				err := db.HandleInsert(rm.d, payload)
				if err != nil {
					return err
				}
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := db.HandleDelete(rm.d, payload)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only redo edit logs")
	}
	return nil
}

// Undo a given log's action.
func (rm *RecoveryManager) Undo(log Log) error {
	switch log := log.(type) {
	case *editLog:
		switch log.action {
		case INSERT_ACTION:
			payload := fmt.Sprintf("delete %v from %s", log.key, log.tablename)
			err := HandleDelete(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case UPDATE_ACTION:
			payload := fmt.Sprintf("update %s %v %v", log.tablename, log.key, log.oldval)
			err := HandleUpdate(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		case DELETE_ACTION:
			payload := fmt.Sprintf("insert %v %v into %s", log.key, log.oldval, log.tablename)
			err := HandleInsert(rm.d, rm.tm, rm, payload, log.id)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("can only undo edit logs")
	}
	return nil
}

// Do a full recovery to the most recent checkpoint on startup.
func (rm *RecoveryManager) Recover() error {
	logs, checkPointPos, _ := rm.readLogs()
	undoList := make(map[uuid.UUID]bool, 0)
	if checkPointPos >= len(logs) || checkPointPos < 0 {
		checkPointPos = 0
	}
	// redo all logs, find undo logs
	for i := checkPointPos; i < len(logs); i++ {
		switch log := logs[i].(type) {
		case *checkpointLog:
			for _, active := range log.ids {
				undoList[active] = true
				rm.tm.Begin(active)
			}
		case *editLog, *tableLog:
			err := rm.Redo(log)
			if err != nil {
				return err
			}
		case *startLog:
			undoList[log.id] = true
			rm.tm.Begin(log.id)
		case *commitLog:
			delete(undoList, log.id)
			rm.tm.Commit(log.id)
		}
	}

	// undo transactions
	for i := len(logs) - 1; len(undoList) > 0; i-- {
		switch log := logs[i].(type) {
		case *editLog:
			if undoList[log.id] == true {
				err := rm.Undo(log)
				if err != nil {
					return err
				}
			}
		case *startLog:
			if undoList[log.id] == true {
				err := rm.tm.Commit(log.id)
				rm.Commit(log.id)
				if err != nil {
					return err
				}
				delete(undoList, log.id)
			}
		}
	}
	return nil
	//panic("function not yet implemented");
}

// Roll back a particular transaction.
func (rm *RecoveryManager) Rollback(clientId uuid.UUID) error {
	logs := rm.txStack[clientId]
	if len(logs) == 0 {
		return nil
	}
	switch logs[0].(type) {
	case *startLog:
		for i := len(logs) - 1; i >= 0; i-- {
			switch l := logs[i].(type) {
			case *editLog:
				err := rm.Undo(l)
				if err != nil {
					return err
				}
			}
		}
	default:
		return errors.New("Invalid rollback: not begin with no start log")
	}
	err := rm.tm.Commit(clientId)
	rm.Commit(clientId)
	if err != nil {
		return err
	}
	return nil
}

// Primes the database for recovery
func Prime(folder string) (*db.Database, error) {
	// Ensure folder is of the form */
	base := strings.TrimSuffix(folder, "/")
	recoveryFolder := base + "-recovery/"
	dbFolder := base + "/"
	if _, err := os.Stat(dbFolder); err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(recoveryFolder, 0775)
			if err != nil {
				return nil, err
			}
			return db.Open(dbFolder)
		}
		return nil, err
	}
	if _, err := os.Stat(recoveryFolder); err != nil {
		if os.IsNotExist(err) {
			return db.Open(dbFolder)
		}
		return nil, err
	}
	os.RemoveAll(dbFolder)
	err := copy.Copy(recoveryFolder, dbFolder)
	if err != nil {
		return nil, err
	}
	return db.Open(dbFolder)
}

// Should be called at end of Checkpoint.
func (rm *RecoveryManager) Delta() error {
	folder := strings.TrimSuffix(rm.d.GetBasePath(), "/")
	recoveryFolder := folder + "-recovery/"
	folder += "/"
	os.RemoveAll(recoveryFolder)
	err := copy.Copy(folder, recoveryFolder)
	return err
}
