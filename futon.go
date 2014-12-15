package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/zond/futon/futon"
)

func main() {
	config := flag.String("dir", filepath.Join(os.Getenv("HOME"), ".futon"), "Which directory should contain the config and cache")

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
	}).Logger(log.New(os.Stderr, "", 0))

	if err := f.Mount(); err != nil {
		panic(err)
	}

}
