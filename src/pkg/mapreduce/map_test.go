package mapreduce

import (
	"bufio"
	"github.com/dgraph-io/badger/v2"
	"os"
	"testing"
)

func TestDecode(t *testing.T) {
	tests := []struct {
		name string
		pair pair
	}{
		{
			name: "basic example",
			pair: pair{Count: 100, Line: 59},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str, err := encode(tt.pair)
			if err != nil {
				t.Fatalf(err.Error())
			}
			got, err := decode(str)
			if err != nil {
				t.Fatalf(err.Error())
			}
			if got.Line != tt.pair.Line || got.Count != tt.pair.Count {
				t.Fatalf("expected line: %v, expected count: %v, actual line: %v, actual count: %v",
					tt.pair.Line, tt.pair.Count, got.Line, got.Count)
			}
		})
	}
}
func TestHash(t *testing.T) {
	tests := []struct {
		name   string
		str    string
		hashID uint32
	}{
		{
			name:   "basic example",
			str:    "rick and morty",
			hashID: 1605679513,
		},
		{
			name:   "basic example",
			str:    "rick",
			hashID: 3324699460,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hashID := hash(tt.str)
			if hashID != tt.hashID {
				t.Fatalf("expected id: %d, actually: %d", tt.hashID, hashID)
			}
		})
	}
}
func TestMapper_Run(t *testing.T) {
	tests := []struct {
		name         string
		partitionNum int
		threadNum    uint8
		fileName     string
	}{
		{
			name:         "basic example",
			partitionNum: 1,
			threadNum:    1,
			fileName:     "testdata/test.txt",
		},
		{
			name:         "multiple partitions",
			partitionNum: 3,
			threadNum:    32,
			fileName:     "testdata/test2.txt",
		},
		{
			name:         "zero case",
			partitionNum: 3,
			threadNum:    32,
			fileName:     "testdata/test3.txt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := os.Open(tt.fileName)
			if err != nil {
				t.Fatal(err)
			}
			scanner := bufio.NewScanner(file)
			mapper, err := NewDefaultMapper(scanner, tt.partitionNum, tt.threadNum)
			if err != nil {
				t.Fatal(err)
			}
			mapper.Run()
			serial := serialMapping(tt.fileName)
			for key, val := range serial {
				hashID := int(hash(key))
				db := mapper.partitions[hashID%mapper.partitionNum]
				err = db.View(func(txn *badger.Txn) error {
					item, err := txn.Get([]byte(key))
					if err != nil {
						return err
					}
					err = item.Value(func(v []byte) error {
						got, err := decode(string(v))
						if err != nil {
							return err
						}
						if got.Line != val.Line || got.Count != val.Count {
							t.Fatalf("expected line: %v, expected count: %v, actual line: %v, actual count: %v",
								val.Line, val.Count, got.Line, got.Count)
						}
						return nil
					})
					if err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			mapper.Clean()
			file.Close()
		})
	}
}
