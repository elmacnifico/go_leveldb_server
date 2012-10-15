package main

import (
	"encoding/json"
	"fmt"
	"github.com/jmhodges/levigo"
	"log"
	"time"
)

type Datapoint struct {
	Average float64
	Count   int
}

type Minute struct {
	Average float64
	Count   int
	Seconds [60]Datapoint
}

type DbWriter struct {
	Db    *levigo.DB
	Input chan *InputSignal
}

func NewDbWriter(db *levigo.DB) *DbWriter {
	dw := &DbWriter{Db: db, Input: make(chan *InputSignal, 1000)}
	go dw.ProcessInput()
	go dw.channelCheker()
	return dw
}

func (dw *DbWriter) channelCheker() {
	for {
		log.Println(len(dw.Input))
		time.Sleep(3 * time.Second)
	}
}

func (dw *DbWriter) ProcessInput() {
	wo := levigo.NewWriteOptions()
	ro := levigo.NewReadOptions()
	defer wo.Close()
	defer ro.Close()

	for signal := range dw.Input {
		//all values get written 3 times
		//minute, hour, day
		//values get accumulated and weighted
		//minute is different as it stores seconds

		//first write minutes
		numOfMin := signal.Time / 60E9
		key := []byte(fmt.Sprintf("%s_M_%s_%d", signal.Id, signal.Host, numOfMin))
		//	log.Printf("key : %s", string(key))
		data, err := dw.Db.Get(ro, key)
		if err != nil {
			panic(err)
		}

		var minute Minute
		err = json.Unmarshal(data, &minute)
		if err != nil {
			//	log.Println("new minute found")
		}
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

		payload, err := json.Marshal(minute)
		log.Println(len(payload))
		if err != nil {
			//there is smth really wrong...some kind of help cry would good
			panic(err)
		}
		//save
		dw.Db.Put(wo, key, payload)
		//log.Println("new minute")
		log.Println(minute)
	}
}
