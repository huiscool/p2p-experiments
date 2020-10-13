package pubsub

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	ipfsaddr "github.com/ipfs/go-ipfs-addr"
	logging "github.com/ipfs/go-log"
	host "github.com/libp2p/go-libp2p-host"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	net "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	protocol "github.com/libp2p/go-libp2p-protocol"
	"github.com/multiformats/go-multihash"
)

var (
	fanoutProtocol    = "fanout/1.0.0"
	fanout            = 10
	fLow              = 8  //fanout low bound
	fHigh             = 12 //fanout up bound
	passiveLen        = 10 //passive peers length
	heartbeatenable   = true
	heartbeatInterval = time.Second * 120
	logger            = logging.Logger("hyparview")
)

//HyparView 这是干嘛？
type HyparView struct {
	// 订阅者
	Subscriptions map[string]*Subscription
	// 被动同胞
	PassivePeers map[peer.ID]*peerstore.PeerInfo
	// pubsub系统
	Psb    *PubSub
	Router *PlumtreeRouter
	host   host.Host
	hDht   *dht.IpfsDHT
	ctx    context.Context
}

//NewHyparView
//host: 任意一台主机？
//topic: 某个主题？
//ctx: 网络上下文
//psb: pubsub系统
//返回：新的HyparView
func NewHyparView(host host.Host, topic string, ctx context.Context, psb *PubSub) *HyparView {
	kadDHT, err := dht.New(context.Background(), host)
	if err != nil {
		logger.Warning(err)
	}
	res := &HyparView{
		host:          host,
		Subscriptions: make(map[string]*Subscription),
		PassivePeers:  make(map[peer.ID]*peerstore.PeerInfo),
		hDht:          kadDHT,
		//Router必须是PlumtreeRouter，需要修改？
		Router: psb.rt.(*PlumtreeRouter),
		ctx:    ctx,
		Psb:    psb}
	//set stream handler
	host.SetStreamHandler(protocol.ID(fanoutProtocol), func(s net.Stream) {
		logger.Debug("get a new Stream connection")

		buf := bufio.NewReader(s)
		logger.Debug("buffer new reader ")
		str, err := buf.ReadString('\n')
		logger.Debug("read string is : ", str)
		if err != nil {
			logger.Error("get error in reading from stream")
		}
		logger.Info("getting eager peers and lazy peers length, router is : ", res.Router)
		pFanout := len(res.Router.EagerPeers[topic]) + len(res.Router.LazyPeers[topic])
		logger.Debug("writing to response fanout : ", pFanout)

		s.Write([]byte(fmt.Sprintf("%d\n", pFanout)))
		//logger.Printf("read: %s\n", str)
		logger.Debugf("read: %s\n", str)
		s.Close()
	})
	return res
}

//ConnectToBootstrapNode 连接引导节点
func (view *HyparView) ConnectToBootstrapNode(bootstrapPeers []string) {
	logger.Debug("bootstrapping...")
	view.hDht.Bootstrap(context.Background())
	var wg sync.WaitGroup
	for _, addr := range bootstrapPeers {
		iaddr, _ := ipfsaddr.ParseString(addr)
		peerinfo, _ := peerstore.InfoFromP2pAddr(iaddr.Multiaddr())
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := view.host.Connect(context.Background(), *peerinfo); err != nil {
				logger.Warning(err)
			} else {
				logger.Info("Connection established with bootstrap node:", *peerinfo)
			}
		}()
	}
	wg.Wait()
}

//AnnouncingTopic 声明主题
func (view *HyparView) AnnouncingTopic(topic string) {
	tctx, _ := context.WithTimeout(view.ctx, time.Second*10)
	//cid是啥？
	// Self-describing content-addressed identifiers for distributed systems
	// 暂时把它当做一个id? 或者一个哈希函数？
	c, _ := cid.NewPrefixV1(cid.Raw, multihash.SHA2_256).Sum([]byte(topic))

	// First, announce ourselves as participating in this topic
	logger.Info("announcing ourselves...")
	logger.Infof("Topic Name is : %s \n", topic)
	if err := view.hDht.Provide(tctx, c, true); err != nil {
		panic(err)
	}
}

//FindPeersInTopic  找某个主题下的同伴， 返回一个切片
func (view *HyparView) FindPeersInTopic(topic string) ([]peerstore.PeerInfo, error) {
	//tctx, _ := context.WithTimeout(view.ctx, time.Second*10)
	tctx, _ := context.WithTimeout(view.ctx, time.Second*10)
	c, _ := cid.NewPrefixV1(cid.Raw, multihash.SHA2_256).Sum([]byte(topic))
	// Now, look for others who have announced
	logger.Info("searching for other peers...")
	peers, err := view.hDht.FindProviders(tctx, c)
	logger.Info("find peers : ", len(peers))
	return peers, err
}

//FindPeersInTopicInExtendingPassive 跟上面一样，时间长点
func (view *HyparView) FindPeersInTopicInExtendingPassive(topic string) ([]peerstore.PeerInfo, error) {
	tctx, _ := context.WithTimeout(view.ctx, time.Second*30)
	c, _ := cid.NewPrefixV1(cid.Raw, multihash.SHA2_256).Sum([]byte(topic))
	// Now, look for others who have announced
	logger.Info("searching for other peers in extending passive peers...")
	peers, err := view.hDht.FindProviders(tctx, c)
	logger.Info("find peers : ", len(peers))
	return peers, err
}

// ConnectToTopicPeers 跟特定同伴连接
func (view *HyparView) ConnectToTopicPeers(peers []peerstore.PeerInfo) {
	//logger.Info("find %d peers \n ", len(peers))
	//peers = view.getFanoutPeers(peers)

	// Now connect to them!
	for _, p := range peers {
		if p.ID == view.host.ID() {
			// No sense connecting to ourselves
			continue
		}

		logger.Info("find peer id : %s\n", p.ID)

		tctx, _ := context.WithTimeout(view.ctx, time.Second*20)
		if err := view.host.Connect(tctx, p); err != nil {
			logger.Error("failed to connect to peer: ", err)
		} else {
			logger.Info("connect to peer : %s\n", p.ID)
		}
	}
}

// SubscribeToTopic 订阅特定主题
func (view *HyparView) SubscribeToTopic(topic string) {
	// 这里为啥要sleep?
	time.Sleep(10 * time.Second)
	sub, err := view.Psb.Subscribe(topic)
	if err != nil {
		panic(err)
	}
	// 加入订阅集合
	view.Subscriptions[topic] = sub
}

// GetFanoutPeers 获得响应的节点？
func (view *HyparView) GetFanoutPeers(peers []peerstore.PeerInfo) []peerstore.PeerInfo {
	//if len(peers) <= fanout {
	//	return peers
	//}
	// 常数
	// fanout            = 10
	nPeers := getRandomNPeers(peers, int(math.Min(float64(len(peers)), float64(fanout*2))))
	num2Peers := make(map[int][]peerstore.PeerInfo) //fanout number -> peers
	for _, p := range nPeers {
		if p.ID == view.host.ID() {
			continue
		}
		// 改变view的passivePeers，除了自身外，其他的都加入到PassivePeers中
		view.PassivePeers[p.ID] = &p
		// getFanout干嘛？
		// 对每个p发送fanout协议，然后接收他们返回的数字，还不知道这个pFan的含义
		pFan := getFanout(context.Background(), view.host, p.ID)
		if pFan == -1 {
			logger.Error("get fanout failed")
			continue
		}
		logger.Info("fanout is : ", pFan)
		if pFan < fanout {
			if num2Peers[pFan] == nil {
				// 根据不同的pFan分别放在不同的列表中
				num2Peers[pFan] = make([]peerstore.PeerInfo, 0, fanout)
			}
			logger.Infof("find peer id : %s\n", p.ID)
			num2Peers[pFan] = append(num2Peers[pFan], p)
		}
	}
	tmp := make([]peerstore.PeerInfo, 0, len(peers))
	copy(tmp, peers)
	res := make([]peerstore.PeerInfo, 0, fanout)
	// 用j来遍历不同的peerInfo的列表
	for _, j := range num2Peers {
		for _, item := range j {
			// 从PassivePeers删除那些响应fanout协议的节点，并加入到返回的列表中
			delete(view.PassivePeers, item.ID)
			res = append(res, item)
			if len(res) == fanout {
				return res
			}
		}
	}
	return res
}

//返回n个随机peer
func getRandomNPeers(peers []peerstore.PeerInfo, n int) []peerstore.PeerInfo {
	for i := range peers {
		j := rand.Intn(i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}
	return peers[0:n]
}

//获得peerID传来的数量吗？
// fanout: 出度
// peerID 联系特定的peerId
func getFanout(ctx context.Context, h host.Host, peerId peer.ID) int {
	s, err := h.NewStream(ctx, peerId, protocol.ID(fanoutProtocol))
	if err != nil {
		logger.Error("new Stream failed: ", "pid: ", peerId.String(), err)
		return -1
	}
	_, wErr := s.Write([]byte("getFanout\n"))
	if wErr != nil {
		logger.Error("write to stream failed")
		return -1
	}
	var str string
	// 为什么是三？
	for i := 0; i < 3; i++ {
		//读取peerID传来的数字
		buf := bufio.NewReader(s)
		str, err = buf.ReadString('\n')
		if err != nil {
			logger.Error("get error in reading from stream")
		}
		if len(str) != 0 {
			res, err := strconv.ParseInt(str[0:len(str)-1], 10, 32)
			if err != nil {
				logger.Error("number convert failed")
			}
			return int(res)
		}
		time.Sleep(1500 * time.Millisecond)
	}
	//读取三次之后，都不行，返回-1
	return -1
}

//StartHeartbeat 开始根据某个主题发送心跳包？
func (view *HyparView) StartHeartbeat(topic string) {
	logger.Debug("calling start heartbeat")
	if !heartbeatenable {
		return
	}
	go func() {
		// initial delay
		time.Sleep(50 * time.Second)

		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C: //每隔一段时间从C这个channel里面得到时间
				view.heartbeat(topic)
			case <-view.ctx.Done():
				logger.Info("view ctx done")
				return
			}
		}
	}()
}

func (view *HyparView) heartbeat(topic string) {
	//每隔一段时间，从passive list中选择一些节点，作为自身的新邻居
	logger.Debug("in heartbeat")
	if view.getSelfFanout() >= fLow { //似乎已经有充足的邻居
		logger.Debug("self fanout is ", view.getSelfFanout())
		return
	}
	logger.Info("passive peers length is ", len(view.PassivePeers))

	for pid, pinfo := range view.PassivePeers {
		//向peer请求出度大小
		// f是pinfo.ID代表节点的出度？
		f := getFanout(context.Background(), view.host, pinfo.ID)
		if f == -1 {
			//无法ping 通，从passivepeers中移除
			delete(view.PassivePeers, pid)
			continue
		}
		if f < fHigh {
			err := view.host.Connect(view.ctx, *pinfo)
			if err != nil {
				logger.Fatal(err)
			} else {
				//连接成功
				// 连接成功也要删除？PassivePeer存的是啥？
				delete(view.PassivePeers, pid)
			}
		}
		if view.getSelfFanout() >= fanout {
			break
		}
	}
	if len(view.PassivePeers) < passiveLen/2 {
		// 没有足够的passivePeer
		logger.Info("fetching passive peers")
		count := 0
		peers, err := view.FindPeersInTopicInExtendingPassive(topic)
		if err == nil {
			for _, p := range peers {
				_, ok := view.Router.p.peers[p.ID] //已经在邻居节点里，跳过
				if p.ID == view.host.ID() || ok {
					continue
				}
				view.PassivePeers[p.ID] = &p
				count++
			}
		} else {
			logger.Error("find passive peers error: ", err)
		}
		logger.Info("fetching peers size : ", count)
	}
}

func (view *HyparView) getSelfFanout() int {
	return len(view.Router.EagerPeers) + len(view.Router.LazyPeers)
}
