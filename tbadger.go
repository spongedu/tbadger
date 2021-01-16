package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/pingcap/badger"
)

const (
	dir = "/Users/felixxdu/pingcap/hackathon_2020/data/mph_20m_8bk_64bv"
	valueDir = "/Users/felixxdu/pingcap/hackathon_2020/data/mph_20m_8bk_64bv"
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
	BatchInsert()
	//scan10()
	//getT()
	//t()
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
	//opts.TableBuilderOptions.BlockSize = 512
	//opts.TableBuilderOptions.MaxTableSize = 8 << 20 * 4
	opts.LevelOneSize = 128 << 20
	opts.TableBuilderOptions.LevelSizeMultiplier = 2
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	var i uint64 = 1

	for {
		err = db.Update(func(txn *badger.Txn) error {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, i)

			value := randStringRunes(64)
			//fmt.Printf("%d|%s\n", i, value)
			return txn.Set(key, []byte(value));
		})
		if err != nil {
			log.Fatal(err)
		}
		i += 1
		if i > 20000000 {
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
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, 12345)


		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err := item.Value()
		if err != nil {
			return err
		}
		fmt.Printf("12345  =: %s\n", val)

		binary.BigEndian.PutUint64(key, 110)

		item, err = txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.Value()
		if err != nil {
			return err
		}
		fmt.Printf("110  =: %s\n", val)

		return nil
	})
	if err != nil {
		log.Fatalf("FATAL: %s", err)
	}
}

func t() {
	i := uint64(1)

	b := make([]byte, 8)
	i = uint64(21)
	binary.BigEndian.PutUint64(b, i)
	fmt.Println(b[:])


	bb := make([]byte, 8)
	i = uint64(123456788)
	binary.BigEndian.PutUint64(bb, i)
	fmt.Println(bb[:])

	fmt.Printf("byte21 > byte 123456788? %d\n", bytes.Compare(b, bb))

	i = uint64(95)
	binary.BigEndian.PutUint64(b, i)
	fmt.Println(b[:])


	i = uint64(100)
	binary.LittleEndian.PutUint64(b, i)
	fmt.Println(b[:])

	i = uint64(1001)
	binary.LittleEndian.PutUint64(b, i)
	fmt.Println(b[:])

	i = uint64(123456788)
	binary.LittleEndian.PutUint64(b, i)
	fmt.Println(b[:])

	i = uint64(18446744073709551614)
	binary.BigEndian.PutUint64(b, i)
	fmt.Println(b[:])

	i = uint64(binary.BigEndian.Uint64(b))
}
