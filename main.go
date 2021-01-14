package main

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/pingcap/badger"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}
var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func main() {
	 insert()
    // exampleW()

	get()
}

func insert() {
	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	opts := badger.DefaultOptions
	opts.Dir = "/Users/felixxdu/test/tbadger/data"
	opts.ValueDir = "/Users/felixxdu/test/tbadger/data/data"
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Insert
	//a := make([]int, 100000, 1000000)

	i := 1
	strconv.Itoa(i)

	for {
		k := fmt.Sprintf("%16d", i)
		v := RandStringRunes(64)
		err = db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(k), []byte(v))
		})
		if err != nil {
			log.Fatal(err)
		}
		i += 1
		//if i > 1000000 {
		if i > 10 {
			break
		}
		if i % 10000 == 0 {
			log.Println(fmt.Sprintf("Processed %d keys", i))
		}
	}

}


func get() {
	opts := badger.DefaultOptions
	opts.Dir = "/Users/felixxdu/test/tbadger/data"
	opts.ValueDir = "/Users/felixxdu/test/tbadger/data/data"
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
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
}


func t1() {
	i := 1
	si := fmt.Sprintf("%16d\n", i)
	j := 11
	sj := fmt.Sprintf("%16d\n", j)
	e := 2
	se := fmt.Sprintf("%16d\n", e)

	fmt.Println(bytes.Compare([]byte(si), []byte(sj)))
	fmt.Println(bytes.Compare([]byte(si), []byte(se)))
	fmt.Println(bytes.Compare([]byte(sj), []byte(se)))
}

func exampleW() {
	opts := badger.DefaultOptions
	opts.Dir = "/Users/felixxdu/test/tbadger/data"
	opts.ValueDir = "/Users/felixxdu/test/tbadger/data/data"
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		_ = txn.Set([]byte("key1"), []byte("value1"))
		_ = txn.Set([]byte("key2"), []byte("value1"))
		_ = txn.Set([]byte("key3"), []byte("value1"))
		_ = txn.Set([]byte("key4"), []byte("value1"))
		_ = txn.Set([]byte("key5"), []byte("value1"))
		_ = txn.Set([]byte("key5"), []byte("value1"))
		_ = txn.Set([]byte("key6"), []byte("value1"))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

}