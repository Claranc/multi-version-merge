package main

import (
	"context"
	"sync"
)

var (
	producerCount int64 = 5
	streamCount   int64 = 5
)

func main() {
	var (
		wg       sync.WaitGroup
		ctx            = context.Background()
		i        int64 = 0
		channels []<-chan *VersionedData
	)

	// start producers
	for ; i < producerCount; i++ {
		p := NewProducer(i, streamCount)
		channels = append(channels, p.DataChan())
		wg.Add(1)
		go func(p *Producer) {
			defer wg.Done()
			p.Start(ctx)
		}(p)
	}

	// start consumer
	c := NewConsumer(channels, streamCount)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Start(ctx)
	}()

	// start verifier
	v := NewVerifier(c.MergedChan())
	wg.Add(1)
	go func() {
		defer wg.Done()
		v.Start(ctx)
	}()

	wg.Wait()
}
