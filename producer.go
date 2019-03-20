package main

import (
	"context"
	"math/rand"
	"sync"
)

// Producer produces versioned data for multi streams
// for a specified stream, versionID never decrease
// but versionIDs between streams may decrease
// Producer produces multi data for a specified stream and specified versionID
// NOTE: you should not modify definition or implementation for Producer
type Producer struct {
	id          int64
	streamCount int64
	ch          chan *VersionedData
}

// NewProducer creates a new Producer
func NewProducer(id, streamCount int64) *Producer {
	p := &Producer{
		id:          id,
		streamCount: streamCount,
		ch:          make(chan *VersionedData),
	}
	return p
}

// Start starts producing versioned data
func (p *Producer) Start(ctx context.Context) {
	var (
		wg sync.WaitGroup
		i  int64 = 0
	)
	for ; i < p.streamCount; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			p.produce(ctx, i)
		}(i)
	}
	wg.Wait()
}

// DataChan returns a chan which can be used to receive produced versioned data
func (p *Producer) DataChan() <-chan *VersionedData {
	return p.ch
}

func (p *Producer) produce(ctx context.Context, streamID int64) {
	var (
		versionID   int64 = 0
		dataCounter       = 5 + rand.Intn(5) // random count
	)
	for {
		data := RandomVersionedData(versionID, p.id, streamID)
		dataCounter--
		if dataCounter < 0 {
			versionID++ // increase version ID
			dataCounter = 5 + rand.Intn(5)
		}
		select {
		case <-ctx.Done():
			return
		case p.ch <- data:
		}
	}
}
