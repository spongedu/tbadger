package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"runtime/pprof"
	"os"
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
	seqGet()
	//scan10()
	//getT()
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
	opts.LevelOneSize = 128 << 20
	opts.TableBuilderOptions.LevelSizeMultiplier = 2
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

func seqGet() {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = valueDir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	var start time.Time
	var end time.Time

	f, err := os. Create("cpu.prof")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	bb := make([][]byte, 20000001, 20000001)
	var t uint64 = 1
	for {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, t)
		bb[t] = b
		t += 1
		if t > 20000000 {
			break
		}
	}
	j := 1
	k := 1
	for {
		start = time.Now()
		for {
			err = db.View(func(txn *badger.Txn) error {
				var i uint64 = 1
				for {
					_, err := txn.Get(bb[i])
					if err != nil {
						return err
					}

					i += 1
					if i > 20000000 {
						break
					}
				}
				return nil
			})
			if err != nil {
				log.Fatalf("FATAL: %s", err)
			}
			j += 1
			if j > 5 {
				break
			}
		}
		end = time.Now()
		log.Printf("ITER[%d] COST %d\n", k, end.Sub(start).Microseconds())
		time.Sleep(2 * time.Second)
		k += 1
		if k > 5 {
			break
		}
	}
}

