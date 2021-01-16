package main

import (
	"encoding/binary"
	"log"
	"math/rand"
	"time"

	"github.com/pingcap/badger"
)

const (
	dir = "/Users/felixxdu/pingcap/hackathon_2020/data/mph_v2"
	valueDir = "/Users/felixxdu/pingcap/hackathon_2020/data/mph_v2"
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

func BatchInsert() {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = valueDir
	opts.TableBuilderOptions.BlockSize = 1024
	opts.TableBuilderOptions.MaxTableSize = 8 << 20 * 4
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
		if i > 2000000 {
			break
		}
		if i % 10000 == 0{
			log.Printf("%d keys already inserted\n", i)
		}
	}
	defer db.Close()
}
