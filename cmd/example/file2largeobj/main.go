package main

import (
	"context"
	"flag"
	"log"

	f2l "github.com/takanoriyanagitani/go-fs2pg2lo"
)

var poolConfigString f2l.PoolConfigString = f2l.PoolConfigStringDefault

func main() {
	var filename string
	flag.StringVar(&filename, "file", "", "path to the file to be saved as a large object")
	flag.Parse()

	if filename == "" {
		log.Println("Please provide a filename using the -file flag.")
		return
	}

	cfg, e := poolConfigString.Parse()
	if nil != e {
		log.Printf("invalid config string: %v\n", e)
		return
	}

	pool, e := cfg.Connect(context.Background())
	if nil != e {
		log.Printf("cannot connect to db: %v\n", e)
		return
	}
	defer pool.Close()

	var store f2l.FileStore = pool.ToFileStoreDefault()
	var saver f2l.FileSaver = store.ToFileSaverDefault()

	e = saver.SaveFile(context.Background(), filename)
	if nil != e {
		log.Printf("cannot save file: %v\n", e)
		return
	}
}
