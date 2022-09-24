package query

import (
	"context"
	"os"

	db "github.com/brown-csci1270/db/pkg/db"
	hash "github.com/brown-csci1270/db/pkg/hash"
	utils "github.com/brown-csci1270/db/pkg/utils"

	errgroup "golang.org/x/sync/errgroup"
)

var DEFAULT_FILTER_SIZE int64 = 1024

// Entry pair struct - output of a join.
type EntryPair struct {
	l utils.Entry
	r utils.Entry
}

// Int pair struct - to keep track of seen bucket pairs.
type pair struct {
	l int64
	r int64
}

// buildHashIndex constructs a temporary hash table for all the entries in the given sourceTable.
func buildHashIndex(
	sourceTable db.Index,
	useKey bool,
) (tempIndex *hash.HashIndex, dbName string, err error) {
	// Get a temporary db file.
	dbName, err = db.GetTempDB()
	if err != nil {
		return nil, "", err
	}
	// Init the temporary hash table.
	tempIndex, err = hash.OpenTable(dbName)
	if err != nil {
		return nil, "", err
	}
	// Build the hash index.
	entries, err := sourceTable.Select();
	if err != nil {
		return nil, "", err
	}
	for _, e := range entries{
		if useKey {
			err = tempIndex.Insert(e.GetKey(), e.GetValue());
		} else {
			err = tempIndex.Insert(e.GetValue(), e.GetKey());
		}
		if err != nil {
			return nil, "", err
		}
	}
	return tempIndex, dbName, err;
}

// sendResult attempts to send a single join result to the resultsChan channel as long as the errgroup hasn't been cancelled.
func sendResult(
	ctx context.Context,
	resultsChan chan EntryPair,
	result EntryPair,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case resultsChan <- result:
		return nil
	}
}

// See which entries in rBucket have a match in lBucket.
func probeBuckets(
	ctx context.Context,
	resultsChan chan EntryPair,
	lBucket *hash.HashBucket,
	rBucket *hash.HashBucket,
	joinOnLeftKey bool,
	joinOnRightKey bool,
) error {
	defer lBucket.GetPage().Put()
	defer rBucket.GetPage().Put()
	// Probe buckets.
	lEntries, err := lBucket.Select();
	if err != nil {
		return err;
	}
	rEntries, err := rBucket.Select();
	if err != nil {
		return err;
	}
	filter := CreateFilter(DEFAULT_FILTER_SIZE);
	for _, tmpEntry := range lEntries {
		filter.Insert(tmpEntry.GetKey());
	}
	for _, rEntry := range rEntries {
		if !filter.Contains(rEntry.GetKey()) {
			continue;
		}
		for _, lEntry := range lEntries {
			if lEntry.GetKey() == rEntry.GetKey() {

				newLeftEntry := hash.HashEntry{}
				if joinOnLeftKey == true {
					newLeftEntry.SetKey(lEntry.GetKey())
					newLeftEntry.SetValue(lEntry.GetValue())
				} else {
					newLeftEntry.SetKey(lEntry.GetValue())
					newLeftEntry.SetValue(lEntry.GetKey())
				}

				newRightEntry := hash.HashEntry{}
				if joinOnRightKey == true {
					newRightEntry.SetKey(rEntry.GetKey())
					newRightEntry.SetValue(rEntry.GetValue())
				} else {
					newRightEntry.SetKey(rEntry.GetValue())
					newRightEntry.SetValue(rEntry.GetKey())
				}

				err = sendResult(ctx, resultsChan, EntryPair{newLeftEntry, newRightEntry})
			}
			if err != nil {
				return err;
			}
		}
	}
	return err;
}

// Join leftTable on rightTable using Grace Hash Join.
func Join(
	ctx context.Context,
	leftTable db.Index,
	rightTable db.Index,
	joinOnLeftKey bool,
	joinOnRightKey bool,
) (chan EntryPair, context.Context, *errgroup.Group, func(), error) {
	leftHashIndex, leftDbName, err := buildHashIndex(leftTable, joinOnLeftKey)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	rightHashIndex, rightDbName, err := buildHashIndex(rightTable, joinOnRightKey)
	if err != nil {
		os.Remove(leftDbName)
		os.Remove(leftDbName + ".meta")
		return nil, nil, nil, nil, err
	}
	cleanupCallback := func() {
		os.Remove(leftDbName)
		os.Remove(leftDbName + ".meta")
		os.Remove(rightDbName)
		os.Remove(rightDbName + ".meta")
	}
	// Make both hash indices the same global size.
	leftHashTable := leftHashIndex.GetTable()
	rightHashTable := rightHashIndex.GetTable()
	for leftHashTable.GetDepth() != rightHashTable.GetDepth() {
		if leftHashTable.GetDepth() < rightHashTable.GetDepth() {
			// Split the left table
			leftHashTable.ExtendTable()
		} else {
			// Split the right table
			rightHashTable.ExtendTable()
		}
	}
	// Probe phase: match buckets to buckets and emit entries that match.
	group, ctx := errgroup.WithContext(ctx)
	resultsChan := make(chan EntryPair, 1024)
	// Iterate through hash buckets, keeping track of pairs we've seen before.
	leftBuckets := leftHashTable.GetBuckets()
	rightBuckets := rightHashTable.GetBuckets()
	seenList := make(map[pair]bool)
	for i, lBucketPN := range leftBuckets {
		rBucketPN := rightBuckets[i]
		bucketPair := pair{l: lBucketPN, r: rBucketPN}
		if _, seen := seenList[bucketPair]; seen {
			continue
		}
		seenList[bucketPair] = true

		lBucket, err := leftHashTable.GetBucketByPN(lBucketPN, hash.NO_LOCK)
		if err != nil {
			return nil, nil, nil, cleanupCallback, err
		}
		rBucket, err := rightHashTable.GetBucketByPN(rBucketPN, hash.NO_LOCK)
		if err != nil {
			lBucket.GetPage().Put()
			return nil, nil, nil, cleanupCallback, err
		}
		group.Go(func() error {
			return probeBuckets(ctx, resultsChan, lBucket, rBucket, joinOnLeftKey, joinOnRightKey)
		})
	}
	return resultsChan, ctx, group, cleanupCallback, nil
}
