package mapreduce

import (
	"github.com/dgraph-io/badger/v2"
	"log"
	"sync"
)

//for each partition retrieve the one non-repeative word with lowest line number
//reduce ans to one with smallest line number
func (mapper MapReduce) getMinLine(db *badger.DB, id int) (str string, lineNum uint64, ok bool) {
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		it.ThreadId = id
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(val []byte) error {
				v, err := decode(string(val))
				if err != nil {
					return err
				}
				if v.Count != 1 {
					return nil
				}

				if !ok {
					lineNum = v.Line
					ok = true
					str = string(k)
					return nil
				}
				if v.Line < lineNum {
					lineNum = v.Line
					str = string(k)
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return str, lineNum, ok
}

//Reduce will compute the first non-repeative word for each partition and combine their results to retrieve the lowest
func (mapper MapReduce) Reduce() (str string, ok bool) {
	var wg sync.WaitGroup
	wg.Add(mapper.partitionNum)
	var lineNum uint64
	for i, db := range mapper.partitions {
		go func(db *badger.DB, i int) {
			currStr, currLine, currOk := mapper.getMinLine(db, i)
			if !currOk {
				wg.Done()
				return
			}
			if !ok {
				lineNum = currLine
				str = currStr
				ok = true
				wg.Done()
				return
			}
			if currLine < lineNum {
				lineNum = currLine
				str = currStr
			}
			wg.Done()
		}(db, i)
	}
	wg.Wait()
	return str, ok
}
