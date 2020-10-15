package main

import (
	"encoding/binary"
	"sync"
	"time"

	pubsub "github.com/huiscool/p2p-experiments/pkg/plumtree"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Processor .
type Processor struct{}

// Request .
type Request = pubsub.Request

// Response .
type Response = pubsub.Response

//RequestSerializer is invoked before publishing
func (p *Processor) RequestSerializer(r Request) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, r.(uint64))
	return out
}

//RequestUnserializer is invoked when a host receive Request
func (p *Processor) RequestUnserializer(stream []byte) Request {
	return binary.BigEndian.Uint64(stream)
}

//ResponseSerializer .
func (p *Processor) ResponseSerializer(r Response) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, r.(uint64))
	return out
}

//ResponseUnserializer .
func (p *Processor) ResponseUnserializer(stream []byte) Response {
	return binary.BigEndian.Uint64(stream)
}

//RequestHandler deals with the request. Each host with get the same request.
func (p *Processor) RequestHandler(r Request) Response {
	return uint64(1)
}

//MergeResponseHandler tells the fetcher how to merge responses which contains local response. It is invoked when the host received all its children's responses.
func (p *Processor) MergeResponseHandler(res []Response) Response {
	out := uint64(0)
	for _, r := range res {
		out += r.(uint64)
	}
	return out
}

// Work function performs concurrent broadcast for several rounds.
func Work(wg *sync.WaitGroup, pid peer.ID, round int, interval int) {
	rt := nodes[pid].Router

	for i := 0; i < round; i++ {
		resp, err := rt.PublishRequest(uint64(0), topic)
		if err != nil {
			panic(err)
		}
		log.Info(pid.ShortString(), ": recv ", resp.(uint64))
		time.Sleep(time.Duration(interval) * time.Millisecond)
	}

	wg.Done()
}
