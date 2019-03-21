package main

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

//并发安全字典存储数据包，一边存储一边取走。key为VersionId,value为当前收到的VersionId下的所有数据包
var mp sync.Map
//记录消费者受到数据的当前最小VersionId
var minId int64 = 0
//记录数据流状态的map. key为一个长度为2的数组，第一个数字代表生产者通道编号，第二个数字代表数据流编号
//value为一个bool型变量，value为true代表该key值代表的数据流已经在发送比当前版本还要大的数据包了
var ChannelState = make(map[[2]int64]bool)
//对ChannelState字典进行写操作的时候的锁
var mu sync.Mutex

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
	//将收到的数据存储到map中的协程
	go c.Receive(ctx)
	//把versionID为0初始化一下，不然下面的OK一直为false
	mp.Store(int64(0), nil)
	for {
		//从数据map中取出当前VersionId下的所有数据包切片，找不到就一直循环等找到了为止
		v1,ok := mp.Load(minId)
		if ok {
			curData,_ := v1.([]*VersionedData)
			//如果当前VersionId下的数据包已经被取完了，就把版本值加1，并重置状态map
			if curData == nil {
				if len(ChannelState) == len(c.dataChannels)*int(c.streamCount) {
					//所有的数据流都已经在发送比当前版本更大的数据包了，清空ChannelState，变成minId+1的状态
					mu.Lock() //加个锁，防止在清空的时候Receive协程写数据
					for key,_ := range ChannelState {
						delete(ChannelState, key)
					}
					mu.Unlock()
					minId++
				}
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
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}

}

func (c *Consumer) Receive(ctx context.Context) {
	for {
		var em *VersionedData //定义当前从生产者数据通道接收到的一个数据包
		t := rand.New(rand.NewSource(time.Now().UnixNano())) //随机种子
		channelNum := t.Intn(len(c.dataChannels))
		select {
		case <-ctx.Done():
			return
		case em = <-c.dataChannels[channelNum]:  //随机从一个生产者通道接收一个数据包(这儿是个性能瓶颈,只能一个一个地接收)
												//因为我不会写从k个通道中同时接收数据的语法(k值不固定),下面示例代码k为3为定值
												// 当k为变量我就不知道该怎么写了
												//select {
												//case <-ch1:
												//case <-ch2:
												//case <-ch3:
												//}
		}
		//取出接收到的数据包的版本号和数据流编号
		dataId := em.VersionID
		streamId := em.StreamID
		//收到的数据包的版本等于当前输出的版本，直接输出
		if dataId == minId {
			c.ch <-em
			continue
		}
		//如果数据包版本大于当前版本,则在状态map中把该数据流的状态置为true
		//表示此数据流已经发送完当前版本的数据包了
		var key = [2]int64{int64(channelNum),streamId}
		//不是并发安全字典，写数据的时候加个锁，防止和清空的时候冲突
		mu.Lock()
		ChannelState[key] = true
		mu.Unlock()
		//存进数据map里
		v1, ok := mp.Load(dataId)
		if ok {
			//如果数据map里有该versionId对应的数据包，就添加到最后
			curData,_ := v1.([]*VersionedData)
			curData = append(curData, em)
			mp.Store(dataId, curData)
		} else {
			//如果是该versionId对应的第一个数据包，就直接存进来
			curData := []*VersionedData{em}
			mp.Store(dataId, curData)
		}

	}
}

// MergedChan returns a chan which can be used to receive merged versioned data
func (c *Consumer) MergedChan() <-chan *VersionedData {
	return c.ch
}
