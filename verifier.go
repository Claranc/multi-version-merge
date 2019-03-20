package main

import (
	"context"
	"fmt"
)

// Verifier verify whether Consumer have merged versioned data
// NOTE: the implementation for Verifier is optional
type Verifier struct {
	ch <-chan *VersionedData
}

// NewVerifier creates a new Verifier
func NewVerifier(ch <-chan *VersionedData) *Verifier {
	v := &Verifier{
		ch: ch,
	}
	return v
}

// Start starts verifying
func (v *Verifier) Start(ctx context.Context) {
	// TODO: implement this to verify data from v.ch
	for{
		select {
		case <-ctx.Done():
			return
		case t := <-v.ch:
			fmt.Printf("versionId = %d, producerId = %d, streamId = %d\n", t.VersionID, t.ProducerID,t.StreamID )
		}
	}

}
