// tool to use the store module
package main

import (
	"fmt"
	"github.com/scusi/scusiStore"
	"io/ioutil"
	"os"
)

func main() {
	fileName := os.Args[1]
	ns := store.New("/home/flow/.scusiStore")
	fmt.Printf("%#v\n", ns)
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	id, err := ns.AddFile(fileName, data)
	if err != nil {
		panic(err)
	}
	fmt.Printf("stored under fileID: %s\n", id)

	l, err := ns.List()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Filelist:\n%+v\n", l)
	for k, v := range l {
		fmt.Printf("%d\t%+v\n", k, v)
	}
}
