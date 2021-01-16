package main

import (
	"encoding/binary"
	"fmt"
	"github.com/pingcap/badger"
	"log"
	"math/rand"
	"testing"
)

func get(db *badger.DB, key uint64) bool {
	found := false
	err := db.View(func(txn *badger.Txn) error {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, key)
		_, err := txn.Get(bs)
		if err != nil {
			return nil
		}
		return nil
	})
	if err != nil {
		log.Fatalf("FATAL: %s", err)
		// log.Printf("NotFound. key: %d, err: %s", key, err.Error())
	} else {
		// log.Printf("Got key: %d, value %d\n", key, binary.BigEndian.Uint32(value))
	}
	found = true
	return found
}


func BenchmarkBadger(b *testing.B) {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = valueDir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	 nFound := 0
	maxValPow1P2 := uint64(2000000)
	for n := 0; n < b.N; n++ {
		key := rand.Uint64() % maxValPow1P2
		found := get(db, key)
		if found {
			nFound ++
		}
	}
	fmt.Printf("-------> Hit rate: %f\n", float64(nFound) / float64(b.N))
}

