// 本程序用于仿真测量EBT(Epidemic Broadcast Tree)在多点同时广播下的稳定性

package main

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	pubsub "github.com/huiscool/p2p-experiments/pkg/pubsub"
	logging "github.com/ipfs/go-log"

	crypto "github.com/libp2p/go-libp2p-core/crypto"
	host "github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	ma "github.com/multiformats/go-multiaddr"
)

var (
	// Num -- 节点总数
	Num *int
	// Fanout -- 节点出度
	Fanout *int
	// Concurrent -- 并发广播节点数
	Concurrent *int
	// Latency -- 延迟(以毫秒计算)
	Latency *int
	// Round -- 广播次数
	Round *int
	// Interval -- 广播时间间隔(以毫秒计算)
	Interval *int
)

var (
	log   = logging.Logger("main")
	topic = "test-topic"
	data  = []byte{1, 2, 3}
)

// global network
var (
	mnet  mocknet.Mocknet
	nodes map[peer.ID]Node
)

// Node .
type Node struct {
	Host   host.Host
	PubSub *pubsub.PubSub
	Router *pubsub.PlumtreeRouter
}

// Processor .
type Processor struct{}
type Request = pubsub.Request
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

func main() {

	// set random seed
	rand.Seed(int64(time.Now().UnixNano()))

	Num = flag.Int("num", 20, "")
	Fanout = flag.Int("fanout", 3, "")
	Concurrent = flag.Int("concurrent", 1, "")
	Latency = flag.Int("latency", 50, "")
	Round = flag.Int("round", 10, "")
	Interval = flag.Int("interval", 100, "")
	flag.Parse()

	logging.SetLogLevel("main", "info")
	logging.SetLogLevel("pubsub", "warn")

	// GenNet
	GenNet(*Num, *Fanout, *Latency)

	// Run
	Run(*Concurrent, *Round, *Interval)

	// Print Statistics

	select {}

}

// GenNet 组网
func GenNet(num int, fanout int, latency int) {
	mnet = mocknet.New(context.Background())

	mnet.SetLinkDefaults(mocknet.LinkOptions{
		Latency: time.Duration(latency) * time.Millisecond,
	})

	nodes = make(map[peer.ID]Node)
	for i := 0; i < num; i++ {
		h, err := GenPeerWithMarshalablePrivKey(mnet)
		if err != nil {
			panic(err)
		}
		rt := pubsub.NewPlumtreeRouter()
		psub, err := pubsub.NewPubSub(context.Background(), h, rt)
		if err != nil {
			panic(err)
		}
		nodes[h.ID()] = Node{
			Host:   h,
			PubSub: psub,
			Router: rt,
		}
	}

	// Each node connects to at least fanout nodes.
	// The actual average fanout will be calculated after the overlay is built.
	allpeers := mnet.Peers()
	for _, peerid := range allpeers {
		neighs := pickupPeersButMyself(peerid, fanout, allpeers)
		for _, neigh := range neighs {
			_, err := mnet.LinkPeers(peerid, neigh)
			if err != nil {
				panic(err)
			}
			_, err = mnet.ConnectPeers(peerid, neigh)
			if err != nil {
				panic(err)
			}
		}
	}

	// log average fanout
	var totalFanout int
	for _, peerid := range allpeers {
		totalFanout += len(mnet.Net(peerid).Peers())
	}
	log.Infof("average fanout: %f", float32(totalFanout)/float32(len(allpeers)))

	// subscribe for test topic
	for _, pid := range allpeers {
		psub := nodes[pid].PubSub
		rt := nodes[pid].Router
		rt.SetProcessor(&Processor{})
		_, err := psub.Subscribe(topic)
		if err != nil {
			panic(err)
		}
	}

	time.Sleep(10 * time.Second)
}

// Run function activates several goroutine to work.
func Run(concurrent int, round int, interval int) {
	senders := pickupPeersButMyself(peer.ID(""), concurrent, mnet.Peers())
	log.Info(senders)
	var wg sync.WaitGroup
	wg.Add(len(senders))
	for _, pid := range senders {
		// 随机化开始时间
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		go Work(&wg, pid, round, interval)
	}
	wg.Wait()
	log.Info("run ok")
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

/*===========================================================================*/
// helper
/*===========================================================================*/

func pickupPeersButMyself(myself peer.ID, n int, peers []peer.ID) (out []peer.ID) {
	if n <= 0 {
		return []peer.ID{}
	}
	if n >= len(peers) {
		n = len(peers) - 1
	}
	out = make([]peer.ID, 0, len(peers))
	for i := 0; i < len(peers); i++ {
		if peers[i] != myself {
			out = append(out, peers[i])
		}
	}
	for i := 0; i < len(out); i++ {
		si := i + rand.Intn(len(out)-i)
		out[i], out[si] = out[si], out[i]
	}
	return out[:n]
}

// GenPeerWithMarshalablePrivKey is the alternative GenPeer(),
// to avoid the unmarshal failure when checking the sign
func GenPeerWithMarshalablePrivKey(mn mocknet.Mocknet) (host.Host, error) {
	sk, _, err := crypto.GenerateECDSAKeyPair(crand.Reader)
	if err != nil {
		return nil, err
	}
	id, err := peer.IDFromPrivateKey(sk)
	if err != nil {
		return nil, err
	}
	suffix := id
	if len(id) > 8 {
		suffix = id[len(id)-8:]
	}
	ip := append(net.IP{}, net.ParseIP("100::")...)
	copy(ip[net.IPv6len-len(suffix):], suffix)
	a, err := ma.NewMultiaddr(fmt.Sprintf("/ip6/%s/tcp/4242", ip))
	if err != nil {
		return nil, fmt.Errorf("failed to create test multiaddr: %s", err)
	}

	h, err := mn.AddPeer(sk, a)
	if err != nil {
		return nil, err
	}

	return h, nil
}
