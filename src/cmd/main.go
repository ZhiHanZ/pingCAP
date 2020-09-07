package main

import (
	"bufio"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"istio.io/pkg/log"
	"os"
	"pingCap/src/pkg/mapreduce"
	"time"
)

type mapreduceOptions struct {
	threadNum    uint8
	partitionNum int
	fileName     string
	cleanup      bool
	validation   bool
}

var (
	loggingOptions = log.DefaultOptions()
)

func parseFlags() (options *mapreduceOptions) {
	// Parse command line flags
	pflag.Int("threads", 32, "number of concurrently working threads")
	pflag.Int("partitions", 20, "number of partitions (hash partition)")
	pflag.String("filename", "", "location of the file which need to be processed")
	pflag.Bool("help", false, "Print usage information")
	pflag.Bool("cleanup", true, "delete all partitions after computing")
	pflag.Bool("validation", false, "validate the output using simple serial program, please"+
		"do not do it for large program")
	pflag.Parse()
	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		log.Fatal("Error parsing command line args: %+v")
	}

	if viper.GetBool("help") {
		pflag.Usage()
		os.Exit(0)
	}
	if viper.GetString("filename") == "" {
		log.Fatalf("please input a valid file name\n")
	}

	viper.SetEnvPrefix("MAP_REDUCE")
	viper.AutomaticEnv()
	options = &mapreduceOptions{
		threadNum:    uint8(viper.GetInt("threads")),
		partitionNum: viper.GetInt("partitions"),
		fileName:     viper.GetString("filename"),
		cleanup:      viper.GetBool("cleanup"),
		validation:   viper.GetBool("validation"),
	}
	return
}

func main() {
	loggingOptions.OutputPaths = []string{"stderr"}
	loggingOptions.JSONEncoding = true
	if err := log.Configure(loggingOptions); err != nil {
		os.Exit(1)
	}
	options := parseFlags()
	file, err := os.Open(options.fileName)
	if err != nil {
		log.Fatal(err)
		file.Close()
		return
	}
	scanner := bufio.NewScanner(file)
	mapper, err := mapreduce.NewDefaultMapper(scanner, options.partitionNum, options.threadNum)
	if err != nil {
		log.Fatal(err)
		file.Close()
		mapper.Clean()
		return
	}
	start := time.Now().UnixNano()
	mapper.Run()
	str, ok := mapper.Reduce()
	if !ok {
		log.Errorf("cannot find a non-repeative string\n")
		file.Close()
		mapper.Clean()
		return
	}
	end := time.Now().UnixNano()
	log.Infof("mapreduce operation used time: %d millisecond", (end - start)/int64(time.Millisecond))
	if options.validation {
		gotStr, gotOk := mapreduce.Validation(options.fileName)
		if ok != gotOk || str != gotStr {
			mapper.Clean()
			file.Close()
			log.Errorf("got string %s, got have string %v, actual string %s, actual has string %v", gotStr, gotOk, str, ok)
		}
	}
	log.Infof("the first non-repeative string is %s ", str)
	if options.cleanup {
		mapper.Clean()
	}
}
