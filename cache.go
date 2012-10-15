package main

import (
	"fmt"
	"log"
	"time"
)

type InputCache struct {
	DbWriter *DbWriter
	Input    chan *InputSignal
	content  map[int64]MinuteCache
}

//cache is a ring of 60 minutes
type MinuteCache struct {
	content map[string]Minute
}

func NewInputCache(db *DbWriter) *InputCache {
	ic := &InputCache{DbWriter: db, Input: make(chan *InputSignal, 100000), content: make(map[int64]MinuteCache, 60)}
	go ic.ProcessInput()
	go ic.FlushCacheToDB()
	go ic.channelCheker()
	return ic
}

func (ic *InputCache) channelCheker() {
	for {
		log.Printf("Input Cache: %d", len(ic.Input))
		log.Printf("Input Cache Size: %d", len(ic.content))
		time.Sleep(3 * time.Second)
	}
}
func (ic *InputCache) FlushCacheToDB() {
	for {
		cacheMin := (time.Now().UnixNano() - ((time.Now().UnixNano() / 3600E9) * 3600E9)) / 60E9
		for key, min := range ic.content[cacheMin].content {
			ic.DbWriter.Input <- &DbInput{Key: key, Minute: &min}
		}
		time.Sleep(3 * time.Second)
	}
}

func (ic *InputCache) ProcessInput() {

	for signal := range ic.Input {
		//0-59 the cache minute
		cacheMin := (signal.Time - ((signal.Time / 3600E9) * 3600E9)) / 60E9
		if ic.content[cacheMin].content == nil {
			ic.content[cacheMin] = MinuteCache{content: make(map[string]Minute, 10000)}
		}

		numOfMin := signal.Time / 60E9
		key := fmt.Sprintf("%s_M_%s_%d", signal.Id, signal.Host, numOfMin)
		minute := ic.content[cacheMin].content[key]

		//log.Println("old minute")
		//log.Println(minute)

		numOfSec := (signal.Time - (numOfMin * 60E9)) / 1E9

		//update second
		second := minute.Seconds[numOfSec]
		sum := second.Average*float64(second.Count) + signal.Value
		second.Count++
		second.Average = sum / float64(second.Count)
		//log.Println(second)

		minute.Seconds[numOfSec] = second

		//update minute
		ic.content[cacheMin].content[key] = minute
		//log.Println("new minute")
		//log.Println(ic)
		cacheMin += 2
		if cacheMin > 59 {
			cacheMin -= 60
		}
		delete(ic.content, cacheMin)
	}
}
