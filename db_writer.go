package main

import (
	"encoding/json"
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

type DbInput struct {
	Minute *Minute
	Key    string
}

type DbWriter struct {
	Db    *levigo.DB
	Input chan *DbInput
}

func NewDbWriter(db *levigo.DB) *DbWriter {
	dw := &DbWriter{Db: db, Input: make(chan *DbInput, 1000)}
	go dw.ProcessInput()
	go dw.channelCheker()
	return dw
}

func (dw *DbWriter) channelCheker() {
	for {
		log.Printf("DB Writer: %d", len(dw.Input))
		time.Sleep(3 * time.Second)
	}
}

func (dw *DbWriter) ProcessInput() {
	wo := levigo.NewWriteOptions()
	ro := levigo.NewReadOptions()
	defer wo.Close()
	defer ro.Close()

	for input := range dw.Input {
		//first write minutes
		key := []byte(input.Key)

		payload, err := json.Marshal(input.Minute)
		log.Println(len(payload))
		if err != nil {
			//there is smth really wrong...some kind of help cry would good
			panic(err)
		}
		//save
		dw.Db.Put(wo, key, payload)
	}
}
