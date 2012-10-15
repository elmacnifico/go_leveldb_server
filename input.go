package main

import (
	"math/rand"
	"strconv"
	"time"
)

type InputSignal struct {
	Value float64
	Time  int64
	Id    string
	Host  string
}

type InputSimulator struct {
	Channel chan *InputSignal
	Workers int
}

func NewInputSimulator(channel chan *InputSignal, interval int64, workers int) *InputSimulator {
	is := &InputSimulator{Channel: channel}
	for i := 0; i < workers; i++ {
		go is.PushSignals(interval, i)
	}
	return is
}

func (is *InputSimulator) PushSignals(interval int64, hostNumber int) {
	interval = interval * 1E6
	for {
		sig := &InputSignal{Host: "host" + strconv.Itoa(hostNumber), Id: "test", Value: rand.Float64() * 1000, Time: time.Now().UTC().UnixNano()}
		is.Channel <- sig
		time.Sleep(time.Duration(interval))
	}
}
