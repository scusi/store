package main

import (
	"flag"
	"fmt"
	"github.com/dchest/blake2b"
	"github.com/peterbourgon/diskv"
	"io/ioutil"
	"log"
	"os"
)

var filename string
var err error
var action string

func init() {
	//flag.StringVar(&filename, "f", "", "file to add to the store")
	//flag.StringVar(&, "", "","")
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

const transformBlockSize = 2 // grouping of chars per directory depth

func blockTransform(s string) []string {
	var (
		sliceSize = len(s) / transformBlockSize
		pathSlice = make([]string, sliceSize)
	)
	for i := 0; i < sliceSize; i++ {
		from, to := i*transformBlockSize, (i*transformBlockSize)+transformBlockSize
		pathSlice[i] = s[from:to]
	}
	return pathSlice
}

func main() {
	flag.Parse()
	action = os.Args[1]
	d := diskv.New(diskv.Options{
		BasePath: "my-diskv-data-directory",
		//Transform:    func(s string) []string { return []string{} },
		Transform: blockTransform,
		//CacheSizeMax: 1024 * 1024, // 1MB
		CacheSizeMax: 1 << 30, // 1GB
	})

	switch action {
	case "add":
		filename = os.Args[2]
		data, err := ioutil.ReadFile(filename)
		check(err)
		key := GenBlake2b32(data)
		if d.Has(key) == false {
			err = d.Write(key, data)
			check(err)
			fmt.Printf("ID: %s filename: %s\n", key, filename)
		} else {
			fmt.Printf("ID: %s is already existing\n", key)
		}
	case "list":
		closingChan := make(chan struct{})
		keyChan := d.Keys(closingChan)
		i := 0
		for s := range keyChan {
			fmt.Printf("%04d: %s\n", i, s)
			i++
		}
		fmt.Printf("%d keys found.\n", i)
	case "prefix":
		prefix := os.Args[2]
		closingChan := make(chan struct{})
		keyChan := d.KeysPrefix(prefix, closingChan)
		i := 0
		for s := range keyChan {
			fmt.Printf("%04d: %s\n", i, s)
			i++
		}

	}
}

func GenBlake2b32(data []byte) (c string) {
	b := blake2b.New256()
	b.Write(data)
	bsum := b.Sum(nil)
	return fmt.Sprintf("%x", bsum)
}
