// tool to use the store module
package main

import (
	"bufio"
	"fmt"
	"github.com/scusi/store"
	"os"
)

func main() {
	// create a new scusiStore under the given path
	ns := store.New("/home/flow/.scusiStore")
	fmt.Printf("%#v\n", ns)

	// if a filename argument was given add that file to scusiStore
	//
	fileName := os.Args[1]
	if fileName != "" {
		// first open a filehandle
		file, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		// turn filehandle into an io.Reader for that file
		fileReader := bufio.NewReader(file)
		// call WriteStream to add the file to scusiStore
		id, err := ns.WriteStream(fileName, fileReader, true)
		if err != nil {
			panic(err)
		}
		fmt.Printf("stored under fileID: %s\n", id)
	} else {
		fmt.Println("No filename given")
		os.Exit(1)
	}

	// get list of all files in scusiStore
	l, err := ns.List()
	if err != nil {
		panic(err)
	}
	// print the list of files
	fmt.Printf("Filelist:\n%+v\n", l)
	for k, v := range l {
		fmt.Printf("%d\t%+v\n", k, v)
	}
}
