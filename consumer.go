package main

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
)

var mp sync.Map
var minId int64 = 0

// Consumer consumes versioned data which produced by multi Producer
// NOTE: the implementation for Consumer is necessary
type Consumer struct {
	dataChannels []<-chan *VersionedData // producer output channels
	streamCount  int64                   // stream count for every producer output chan
	ch           chan *VersionedData     // merged result chan
}

// NewConsumer creates a new Consumer
func NewConsumer(channels []<-chan *VersionedData, streamCount int64) *Consumer {
	c := &Consumer{
		dataChannels: channels,
		streamCount:  streamCount,
		ch:           make(chan *VersionedData),
	}
	return c
}

// Start starts consuming versioned data
func (c *Consumer) Start(ctx context.Context) {
	// TODO: implement this to consume data from c.dataChannels, and merged result should send to c.ch
	go c.Receive(ctx)
	log.Println("receive start:")
	count := 0;
	for {
		v1,ok := mp.Load(minId)
		if ok {
			curData,_ := v1.([]*VersionedData)
			sendData := curData[0]
			if len(curData) == 1 {
				mp.Delete(minId)
			} else {
				curData = curData[1:]
			}
			mp.Store(minId, curData)
			c.ch<- sendData
		} else {
			//fmt.Println("cannot find versionID of ", minId)
			if count < 20 {
				time.Sleep(time.Millisecond*10)
			} else {
				minId++
			}
		}
	}

}

func (c *Consumer) Receive(ctx context.Context) {
	for {
		var em *VersionedData
		t := rand.New(rand.NewSource(time.Now().UnixNano()))
		temp := t.Intn(5)
		//fmt.Println("rand = ", temp)
		select {
		case <-ctx.Done():
			return
		case em = <-c.dataChannels[temp]:
		}
		num := em.VersionID
		//fmt.Println("num = ", num)
		v1, ok := mp.Load(num)
		if ok {
			curData,_ := v1.([]*VersionedData)
			curData = append(curData, em)
			mp.Store(num, curData)
			//fmt.Printf("key = %d, value = %v /n", num, *curData[1])
		} else {
			curData := []*VersionedData{em}
			mp.Store(num, curData)
			//fmt.Println("add data to map")
		}

	}
}

// MergedChan returns a chan which can be used to receive merged versioned data
func (c *Consumer) MergedChan() <-chan *VersionedData {
	return c.ch
}
