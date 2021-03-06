package main

import (
	"github.com/jmhodges/levigo"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(6)

	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(1E9))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open("tmp/db", opts)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	writer := NewDbWriter(db)
	cache := NewInputCache(writer)
	NewInputSimulator(cache.Input, 500, 1)
	select {}
}
