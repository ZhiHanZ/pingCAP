package mapreduce

import (
	"bufio"
	"os"
	"testing"
)

func TestMapper_Reduce(t *testing.T) {
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
			gotStr, gotOk := mapper.Reduce()
			str, ok := serialReducer(serialMapping(tt.fileName))
			if ok != gotOk || str != gotStr {
				mapper.Clean()
				file.Close()
				t.Errorf("got string %s, got have string %v, actual string %s, actual has string %v", gotStr, gotOk, str, ok)
			}
			mapper.Clean()
			file.Close()
		})
	}
}
