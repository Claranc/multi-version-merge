package main

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
)

//并发安全字典存储数据包，一边存储一边取走。key为VersionId,value为当前收到的VersionId下的所有数据包
var mp sync.Map
//记录消费者受到数据的当前最小VersionId
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
	//将收到的数据存储到map中
	go c.Receive(ctx)
	//等待一小会，等map中已经开始有一些数据了再开始处理
	time.Sleep(time.Millisecond*200)
	log.Println("receive start:")
	for {
		//取出map中当前VersionId下的所有数据包切片
		v1,ok := mp.Load(minId)
		if ok {
			curData,_ := v1.([]*VersionedData)
			//如果当前VersionId下的数据包已经被取完了，就把key值加1，取下一个版本号的value
			if curData == nil {
				minId++
				continue
			}
			//取走当前VersionId下的数据包切片中的第一个数据，发送到c.ch
			sendData := curData[0]
			//如果这恰好是最后一个数据，取走后就把value设为空值，否则就删掉第一个
			if len(curData) == 1 {
				curData = nil
			} else {
				curData = curData[1:]
			}
			//重新存进map里
			mp.Store(minId, curData)
			c.ch<- sendData
		} else {
			//如果没找到key对应的value，表明还没有该版本的数据包发送过来，跳过吧
			minId++
		}
	}

}

func (c *Consumer) Receive(ctx context.Context) {
	for {
		var em *VersionedData //定义当前从生产者数据通道接收到的一个数据包
		t := rand.New(rand.NewSource(time.Now().UnixNano())) //随机种子
		select {
		case <-ctx.Done():
			return
		case em = <-c.dataChannels[t.Intn(len(c.dataChannels))]:  //随机从一个生产者通道接收一个数据包
		}
		//将该数据包存进map里
		num := em.VersionID
		v1, ok := mp.Load(num)
		if ok {
			//如果map里有该versionId对应的数据包，就添加到最后
			curData,_ := v1.([]*VersionedData)
			curData = append(curData, em)
			mp.Store(num, curData)
		} else {
			//如果是该versionId对应的第一个数据包，就直接存进来
			curData := []*VersionedData{em}
			mp.Store(num, curData)
		}

	}
}

// MergedChan returns a chan which can be used to receive merged versioned data
func (c *Consumer) MergedChan() <-chan *VersionedData {
	return c.ch
}
