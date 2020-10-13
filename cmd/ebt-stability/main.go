// 本程序用于仿真测量EBT(Epidemic Broadcast Tree)在多点同时广播下的稳定性

package main

import (
	"context"
	crand "crypto/rand"
	"flag"
	"fmt"
	"math/rand"
	"net"
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
	// Concurrent -- 并发广播数
	Concurrent *int
	// Latency -- 延迟(以毫秒计算)
	Latency *int
)

var (
	log = logging.Logger("main")
)

// global network
var (
	mnet  mocknet.Mocknet
	nodes map[string]Node
)

// Node .
type Node struct {
	Host   host.Host
	PubSub *pubsub.PubSub

	// statistic
}

func main() {

	// set random seed
	rand.Seed(int64(time.Now().UnixNano()))

	Num = flag.Int("num", 20, "")
	Fanout = flag.Int("fanout", 3, "")
	Concurrent = flag.Int("curr", 1, "")
	Latency = flag.Int("latency", 50, "")
	flag.Parse()

	logging.SetLogLevel("main", "info")

	// GenNet
	GenNet(*Num, *Fanout, *Latency)

	// Run

	// Print Statistics

}

// GenNet 组网
func GenNet(num int, fanout int, latency int) {
	mnet = mocknet.New(context.Background())

	mnet.SetLinkDefaults(mocknet.LinkOptions{
		Latency: time.Duration(latency) * time.Millisecond,
	})

	nodes = make(map[string]Node)
	for i := 0; i < num; i++ {
		h, err := GenPeerWithMarshalablePrivKey(mnet)
		if err != nil {
			panic(err)
		}
		psub, err := pubsub.NewPlumtreeSub(context.Background(), h)
		if err != nil {
			panic(err)
		}
		nodes[h.ID().String()] = Node{
			Host:   h,
			PubSub: psub,
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
