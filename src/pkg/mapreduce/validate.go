package mapreduce

import (
	"bufio"
	"log"
	"os"
)

//for small dataset validation only
func serialMapping(fileName string) map[string]pair {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(file)
	ans := make(map[string]pair)
	var lineNum uint64 = 0
	for scanner.Scan() {
		key := scanner.Text()
		if _, ok := ans[key]; !ok {
			ans[key] = pair{Line: lineNum, Count: 1}
		} else {
			ans[key] = pair{Line: min(lineNum, ans[key].Line), Count: ans[key].Count + 1}
		}
		lineNum++
	}
	return ans
}

//for small dataset validation
func serialReducer(mapped map[string]pair) (str string, ok bool) {
	var lineNum uint64
	for k, v := range mapped {
		if v.Count != 1 {
			continue
		}
		if !ok {
			lineNum = v.Line
			ok = true
			str = k
		}
		if v.Line < lineNum {
			str = k
			lineNum = v.Line
		}
	}
	return str, ok
}

//Validation is for checking results for small sized file
func Validation(fileName string) (str string, ok bool) {
	return serialReducer(serialMapping(fileName))
}
