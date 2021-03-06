package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/pingcap/badger"
)

const (
	dir = "/tmp/cbadger_test"
	valueDir = "/tmp/cbadger_test"
	//dir =  "/Users/felixxdu/test/tbadger_data"
	//valueDir = "/Users/felixxdu/test/tbadger_data/data"
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
	//BatchInsert()
	//scan10()
	getT()
}

func insert1() {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = valueDir
	opts.TableBuilderOptions.BlockSize = 1024
	opts.TableBuilderOptions.MaxTableSize = 8 << 20 * 4
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

func scan10() {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = valueDir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	i := 0
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
			fmt.Printf("|key=%s|value=%s|\n", k, v)
			i += 1
			if i > 10 {
				break
			}
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
	opts.TableBuilderOptions.BlockSize = 1024
	opts.TableBuilderOptions.MaxTableSize = 8 << 20 * 4
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	i := 1

	for {
		err = db.Update(func(txn *badger.Txn) error {
			key := fmt.Sprintf("%16d", i)
			value := randStringRunes(64)
			fmt.Printf("%s|%s\n", key, value)
			return txn.Set([]byte(key), []byte(value));
		})
		if err != nil {
			log.Fatal(err)
		}
		i += 1
		if i > 10000 {
			break
		}
		if i % 10000 == 0{
			log.Printf("%d keys already inserted\n", i)
		}
	}
	defer db.Close()
}

func getT() {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = valueDir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	err = db.View(func(txn *badger.Txn) error {
		/*
		item, err := txn.Get([]byte("11"))
		if err != nil {
			return err
		}
		val, err := item.Value()
		if err != nil {
			return err
		}
		fmt.Printf("11  =: %s\n", val)

		 */

		item, err := txn.Get([]byte(fmt.Sprintf("%16d", 919)))
		if err != nil {
			return err
		}
		val, err := item.Value()
		if err != nil {
			return err
		}
		fmt.Printf("919  =: %s\n", val)
		/*

		item, err = txn.Get([]byte("9"))
		if err != nil {
			return err
		}
		val, err = item.Value()
		if err != nil {
			return err
		}
		fmt.Printf("9  =: %s\n", val)

		 */
		return nil
	})
	if err != nil {
		log.Fatalf("FATAL: %s", err)
	}
}
