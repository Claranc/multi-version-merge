package main

import "math/rand"

// VersionedData represents data with version info
type VersionedData struct {
	VersionID  int64
	ProducerID int64
	StreamID   int64
	Data       string
}

// RandomVersionedData generates a random VersionedData
func RandomVersionedData(versionID, producerID, streamID int64) *VersionedData {
	data := &VersionedData{
		VersionID:  versionID,
		ProducerID: producerID,
		StreamID:   streamID,
		Data:       genRandomString(),
	}
	return data
}

func genRandomString() string {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, rand.Intn(5)+5)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
