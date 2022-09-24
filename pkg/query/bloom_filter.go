package query

import (
	bitset "github.com/bits-and-blooms/bitset"
	hash "github.com/brown-csci1270/db/pkg/hash"
)

type BloomFilter struct {
	size int64
	bits *bitset.BitSet
}

// CreateFilter initializes a BloomFilter with the given size.
func CreateFilter(size int64) *BloomFilter {
	res := BloomFilter{size, bitset.New(uint(size))};
	return &res;
}

// Insert adds an element into the bloom filter.
func (filter *BloomFilter) Insert(key int64) {
	h1 := hash.XxHasher(key, filter.size);
	h2 := hash.MurmurHasher(key, filter.size);
	filter.bits.Set(h1);
	filter.bits.Set(h2);
}

// Contains checks if the given key can be found in the bloom filter/
func (filter *BloomFilter) Contains(key int64) bool {
	h1 := hash.XxHasher(key, filter.size);
	h2 := hash.MurmurHasher(key, filter.size);
	return filter.bits.Test(h1) && filter.bits.Test(h2);
}
