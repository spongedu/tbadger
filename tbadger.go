package main

import (
	"encoding/binary"
	"log"
	"math/rand"
	"time"

	"github.com/pingcap/badger"
)

const (
	dir = "/Users/felixxdu/pingcap/hackathon_2020/data/20m_8bk_64bv"
	valueDir = "/Users/felixxdu/pingcap/hackathon_2020/data/20m_8bk_64bv"
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

func seqGet() {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = valueDir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	start := time.Now()
	defer func() {
		end := time.Now()
		log.Printf("COST %d\n", end.Sub(start).Microseconds())
	}()


	// f, err := os.Create("cpu.prof")
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()

	j := 1
	b := make([]byte, 8)
	for {
		var i uint64 = 1
		for {
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

				binary.BigEndian.PutUint64(b, i)
				_, err := txn.Get(b)
				if err != nil {
					return err
				}
				//, err = item.Value()
				// if err != nil {
				// 	return err
				// }
				//fmt.Printf("919  =: %s\n", val)
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
			i += 1
			if i > 20000000 {
				break
			}
		}
		j += 1
		if j > 5 {
			break
		}
	}
}

