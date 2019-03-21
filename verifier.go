package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
)

//记录通道中的当前versionId
var curId int64 = 0

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
	output, err := os.OpenFile("log.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC,0644)
	defer output.Close()

	if err != nil {
		log.Fatal("open file failed")
	}
	outputwriter := bufio.NewWriter(output)
	for{
		select {
		case <-ctx.Done():
			return
		case t:=<-v.ch:
			//将输出通道的数据包数据打印到日志中
			s := fmt.Sprintf("versionId = %d, producerId = %d, streamId = %d, data = %s\n", t.VersionID, t.ProducerID,t.StreamID, t.Data )
			outputwriter.WriteString(s)
			outputwriter.Flush()
			if(t.VersionID > curId) {
				//如果收到更高版本的数据，更新当前的curId
				curId = t.VersionID
			} else if (t.VersionID < curId) {
				//如果出现了新的数据包小于当前versionId，则代表错误，程序编写有问题
				log.Fatal("output wrong")
			}
		}
	}

}
