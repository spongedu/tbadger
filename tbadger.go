package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/pingcap/badger"
)

const (
	dir =  "/Users/felixxdu/test/tbadger/data"
	valueDir = "/Users/felixxdu/test/tbadger/data/data"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func main() {
	BatchInsert()
	scan()
}

func insert1() {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = valueDir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Update(func(txn *badger.Txn) error {
		_  = txn.Set([]byte("key1"),[]byte("value1"));
		_  = txn.Set([]byte("key2"),[]byte("value2"));
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}

func scan() {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = valueDir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, err := item.Value()
			if err != nil {
				return err
			}
			fmt.Printf("key=%s, value=%s\n", k, v)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}

func BatchInsert() {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = valueDir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	i := 1

	for {
		err = db.Update(func(txn *badger.Txn) error {
			key := fmt.Sprintf("%16d", i)
			value := randStringRunes(64)
			_ = txn.Set([]byte(key), []byte(value));
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
		i += 1
		if i > 10000 {
			break
		}
		if i % 1000 == 0{
			log.Printf("%d keys already inserted\n", i)
		}
	}
	defer db.Close()
}
