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
	auth := flag.String("auth", filepath.Join(os.Getenv("HOME"), ".futon", "auth"), "Which file should contain the auth token")

	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}

	mountpoint := flag.Arg(0)

	f := futon.New(mountpoint, *auth, func(url string) (result string) {
		fmt.Printf("Go to %#v\n", url)
		fmt.Scanln(&result)
		return
	}).Logger(log.New(os.Stderr, "", 0))

	if err := f.Mount(); err != nil {
		panic(err)
	}

}
