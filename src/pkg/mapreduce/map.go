package mapreduce

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	badger "github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/zhangxinngang/murmur"
	"log"
	"os"
	"sync"
)

type token struct {
	key     string
	lineNum uint64
}
//MapReduce is the main structure for computing the the first non-repeative word
type MapReduce struct {
	channel       *chan token
	partitionNum  int
	workQueueSize uint64
	threadNum     uint8
	scanner       *bufio.Scanner
	partitions    []*badger.DB
}
type pair struct {
	Count uint64
	Line  uint64
}
//NewDefaultMapper will initialized partitions with snappy compression and set up a new computing framework
func NewDefaultMapper(scanner *bufio.Scanner, shardNum int, threadNum uint8) (MapReduce, error) {
	channel := make(chan token, 1024)
	mapper := MapReduce{channel: &channel, partitionNum: shardNum, workQueueSize: 1024, threadNum: threadNum}
	mapper.partitions = make([]*badger.DB, 0)
	mapper.scanner = scanner
	for i := 0; i < shardNum; i++ {
		option := badger.DefaultOptions(fmt.Sprintf("shard_%d", i))
		option.WithCompression(options.Snappy)
		option.Logger = nil
		db, err := badger.Open(option)
		if err != nil {
			log.Fatal(err)
		}
		mapper.partitions = append(mapper.partitions, db)
	}
	return mapper, nil
}
func hash(str string) uint32 {
	idx := murmur.Murmur3([]byte(str))
	return idx
}

func encode(pair pair) (str string, err error) {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err = e.Encode(pair)
	if err != nil {
		return str, err
	}
	return base64.StdEncoding.EncodeToString(b.Bytes()), nil
}
func decode(str string) (p pair, err error) {
	p = pair{}
	by, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return p, err
	}
	b := bytes.Buffer{}
	b.Write(by)
	d := gob.NewDecoder(&b)
	err = d.Decode(&p)
	if err != nil {
		return p, err
	}
	return p, err
}
func (mapper *MapReduce) producer(wg *sync.WaitGroup) {
	var lineNum uint64 = 0
	for mapper.scanner.Scan() {
		*mapper.channel <- token{mapper.scanner.Text(), lineNum}
		lineNum++
		wg.Add(1)
	}
	close(*mapper.channel)
}
//Run will start parallel IO operation and using work pool to update partition
func (mapper *MapReduce) Run() {
	var wg sync.WaitGroup
	for i := 0; i < int(mapper.threadNum); i++ {
		go mapper.worker(&wg)
	}
	mapper.producer(&wg)

	wg.Wait()
}
func (mapper *MapReduce) worker(wg *sync.WaitGroup) {
	for token := range *mapper.channel {
		mapper.update(token)
		wg.Done()
	}
}
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
		return b
}

//basic map reduce operation
//token contains information about line number and line word content
//here we want to put token into given shard according to their hash value
func (mapper *MapReduce) update(tk token) {
	hashID := int(hash(tk.key))
	db := mapper.partitions[hashID%mapper.partitionNum]
	err := db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(tk.key))
		if err != nil {
			if err != badger.ErrKeyNotFound {
				return err
			}
			val, err := encode(pair{Count: 1, Line: tk.lineNum})
			if err != nil {
				return err
			}
			if err := txn.Set([]byte(tk.key), []byte(val)); err != nil {
				return err
			}
		} else {
			err = item.Value(func(val []byte) error {
				oldVal, err := decode(string(val))
				if err != nil {
					return err
				}
				currVal, err := encode(pair{Count: oldVal.Count + 1, Line: min(tk.lineNum, oldVal.Line)})
				if err != nil {
					return nil
				}
				if err := txn.Set([]byte(tk.key), []byte(currVal)); err != nil {
					return err
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
		//retry policy
		mapper.update(tk)
	}
}
//Clean will delete all partitions
func (mapper *MapReduce) Clean() {
	for i := 0; i < mapper.partitionNum; i++ {
		if err := mapper.partitions[i].DropAll(); err != nil {
			log.Fatal(err)
		}
	}
	for i, db := range mapper.partitions {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
		if err = os.RemoveAll(fmt.Sprintf("shard_%d", i)); err != nil {
			log.Fatal(err)
		}
	}
}
