package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/zond/futon/futon"
)

node save and rawsave should use the buckets type
loadblock should clean out old blocks as long as the total number of stored bytes in the blocks bucket is greater than maxCache

func main() {
	config := flag.String("dir", filepath.Join(os.Getenv("HOME"), ".futon"), "Which directory should contain the config and cache")
	loglevel := flag.Int("loglevel", 4, "How much to log. Higher is more.")

	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}

	mountpoint := flag.Arg(0)

	f := futon.New(mountpoint, *config, func(url string) (result string) {
		fmt.Printf("Go to %#v\nEnter the code: ", url)
		fmt.Scanln(&result)
		return
	}).Logger(log.New(os.Stderr, "", 0)).Loglevel(*loglevel)

	if err := f.Mount(); err != nil {
		panic(err)
	}

}
