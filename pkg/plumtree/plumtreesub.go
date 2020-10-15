package pubsub

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	pb "github.com/huiscool/p2p-experiments/pkg/plumtree/pb"
	logging "github.com/ipfs/go-log"
	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	protocol "github.com/libp2p/go-libp2p-protocol"
	uuid "github.com/nu7hatch/gouuid"
	"github.com/patrickmn/go-cache"
)

const (
	//PlumtreeSubID 协议ID
	PlumtreeSubID = protocol.ID("/plumtreesub/1.0.0")
)

var (
	//FanoutSize 节点出度
	FanoutSize = 10
	// PlumtreeMsgCacheSize 生成树缓存？
	PlumtreeMsgCacheSize = 10
	// CacheDuration 缓存存留时间？
	CacheDuration = 1 * time.Minute
	// ClearCacheDuration 这个时间之后清理缓存？
	ClearCacheDuration        = CacheDuration * 2
	WaitResponseTime          = 2 * time.Second //等待孩子节点的返回时间
	PlumtreeHeartbeatInterval = 5 * time.Second
	PullTimeout               = 500 * time.Millisecond //在这段时间内等待父节点的消息，如果没收到，则从ihave的节点里面拉取
	StepsOptimization         = false
	StepOptThreshold          = 2 //跳数优化阈值，相差>2 需要优化
)

// NewPlumtreeRouter return an exposed PlumtreeRouter
func NewPlumtreeRouter() *PlumtreeRouter {
	rt := &PlumtreeRouter{
		peers:             make(map[peer.ID]protocol.ID),
		EagerPeers:        make(map[string]map[peer.ID]struct{}),
		LazyPeers:         make(map[string]map[peer.ID]struct{}),
		CacheMessage:      cache.New(CacheDuration, ClearCacheDuration),
		childrendataCache: cache.New(CacheDuration, ClearCacheDuration),
		CollectDoneCache:  cache.New(CacheDuration, ClearCacheDuration),
		fatherMap:         cache.New(CacheDuration, ClearCacheDuration),
		TimeCost:          cache.New(CacheDuration, ClearCacheDuration),
		ResultCache:       cache.New(CacheDuration, ClearCacheDuration),
		QueryIDCache:      cache.New(CacheDuration, ClearCacheDuration),
		EagerReceivedTime: cache.New(CacheDuration, ClearCacheDuration),
		proc:              &EmptyProcessor{},
		log:               &loggerWithID{ZapEventLogger: log},
	}
	rt.heartbeatTimer() //开启定时任务
	return rt
}

//NewPlumtreeSub returns a new PubSub object using the PlumtreeRouter.
func NewPlumtreeSub(ctx context.Context, h host.Host, opts ...Option) (*PubSub, error) {
	rt := NewPlumtreeRouter()
	return NewPubSub(ctx, h, rt, opts...)
}

//PlumtreeRouter 实现了PubSubRouter的接口, 从而可以使用Plumtree进行PubSub
//需不需要HyparView? 似乎不需要
type PlumtreeRouter struct {
	p                 *PubSub
	proc              Processor
	peers             map[peer.ID]protocol.ID // peer protocols
	LazyPeers         map[string]map[peer.ID]struct{}
	EagerPeers        map[string]map[peer.ID]struct{}
	CacheMessage      *cache.Cache //msgID->msg
	fatherMap         *cache.Cache //peer.ID
	MapID2No          map[string]int
	childrendataCache *cache.Cache
	CollectDoneCache  *cache.Cache
	TimeCost          *cache.Cache
	ResultCache       *cache.Cache
	QueryIDCache      *cache.Cache
	EagerReceivedTime *cache.Cache //msgID -> time stamp
	log               *loggerWithID
}

// Protocols returns the list of protocols supported by the router.
func (pr *PlumtreeRouter) Protocols() []protocol.ID {
	return []protocol.ID{PlumtreeSubID}
}

//Attach is invoked by the PubSub constructor to attach the router to a
// freshly initialized PubSub instance.
func (pr *PlumtreeRouter) Attach(p *PubSub) {
	pr.p = p
	pr.log.pid = p.host.ID()
}

// AddPeer notifies the router that a new peer has been connected.
func (pr *PlumtreeRouter) AddPeer(pid peer.ID, ptoid protocol.ID) {
	pr.log.Debugf("PEERUP: Add new peer %s using %s", pid, ptoid)
	pr.peers[pid] = ptoid
}

// RemovePeer notifies the router that a peer has been disconnected.
func (pr *PlumtreeRouter) RemovePeer(p peer.ID) {
	pr.log.Debugf("PEERDOWN: Remove disconnected peer %s", p)
	delete(pr.peers, p)
	for _, peers := range pr.LazyPeers {
		delete(peers, p)
	}
	for _, peers := range pr.EagerPeers {
		delete(peers, p)
	}
	//替换新的Lazy Peers,todo
}

//TODO:定时从Passive中选择延迟最低的节点且出度较小的节点，作为新的邻居
func (pr *PlumtreeRouter) getCandidatesFromPassive(topic string, count int, filter func(peer.ID) bool) []peer.ID {
	return nil
}

// HandleRPC is invoked to process control messages in the RPC envelope.
// It is invoked after subscriptions and payload messages have been processed.
func (pr *PlumtreeRouter) HandleRPC(rpc *RPC) {
	ctl := rpc.GetControl()
	if ctl == nil {
		return
	}
	pr.handlePrune(rpc.from, ctl)
	iwant, graft := pr.handleIHave(rpc.from, ctl)
	ihave := pr.handleIWant(rpc.from, ctl)
	prune := pr.handleGraft(rpc.from, ctl)
	if len(iwant) == 0 && len(graft) == 0 && len(ihave) == 0 && len(prune) == 0 {
		return
	}
	out := rpcWithControl(ihave, nil, iwant, graft, prune)
	pr.sendRPC(rpc.from, out)
}

func (pr *PlumtreeRouter) handleIHave(p peer.ID, ctl *pb.ControlMessage) ([]*pb.ControlIWant, []*pb.ControlGraft) {
	var iwant, graft []string
	for _, iHave := range ctl.GetIhave() {
		pr.log.Debug("handling IHave")
		topic := iHave.GetTopicID()
		if _, ok := pr.p.topics[topic]; !ok {
			return nil, nil
		}
		//如果超时时间内，没有收到消息ID对应的消息，发送graft和iwant控制消息
		//数组的大小，始终为1，因为要兼容之前的消息类型
		for _, msgID := range iHave.GetMessageIDs() {
			//TODO:根据节点的跳数做优化，仅在节点数量多，且查询的数据量很大的情况下用。后续再实现
			// 这里需要异步，避免阻塞pubsub主流程
			go func(p peer.ID, msgID string) {
				<-time.After(PullTimeout)
				if !pr.p.seenMessages.Has(msgID) {
					pr.log.Infof("not seen timeout: from=%s msgid=%s", p.ShortString(), msgID)
					iwant = append(iwant, msgID)
					graft = append(graft, topic)

					ePeers := pr.EagerPeers[topic]
					lPeers := pr.LazyPeers[topic]
					if _, ok := ePeers[p]; !ok {
						// 需要放到eval里面避免并发修改map
						pr.p.eval <- func() {
							//eager list does not contain this peer
							pr.log.Infof("GRAFT: to %s", p.ShortString())
							ePeers[p] = struct{}{}
							pr.tagPeer(p, topic)
							if _, ok := lPeers[p]; ok {
								delete(lPeers, p)
							}
						}
					}
				}
			}(p, msgID)
		}
	}
	if len(iwant) == 0 && len(graft) == 0 {
		return nil, nil
	}
	cIwant := make([]*pb.ControlIWant, 0, len(iwant))
	cGraft := make([]*pb.ControlGraft, 0, len(graft))
	for _, topic := range graft {
		cGraft = append(cGraft, &pb.ControlGraft{TopicID: &topic})
	}
	cIwant = append(cIwant, &pb.ControlIWant{MessageIDs: iwant})
	//return cIwant, cGraft
	//这里仔细考虑过，保证一次容错查询的延迟，来牺牲正常情况下的延迟不值得
	//所以，当查询过程中有容错和优化时，作废本次查询，根节点检测到后，重新发送查询请求
	return nil, cGraft
}

func (pr *PlumtreeRouter) handlePrune(p peer.ID, ctl *pb.ControlMessage) {
	for _, prune := range ctl.GetPrune() {
		pr.log.Info("handling Prune")
		topic := prune.GetTopicID()
		if _, ok := pr.LazyPeers[topic]; !ok {
			pr.LazyPeers[topic] = make(map[peer.ID]struct{})
		}
		peers, ok := pr.EagerPeers[topic]
		lPeers := pr.LazyPeers[topic]
		if ok {
			pr.log.Infof("PRUNE to %s \n", p.ShortString())
			delete(peers, p)
			lPeers[p] = struct{}{}
		}
	}
}

func (pr *PlumtreeRouter) handleGraft(p peer.ID, ctl *pb.ControlMessage) []*pb.ControlPrune {
	var prune []string
	for _, graft := range ctl.GetGraft() {
		pr.log.Info("handling graft")
		topic := graft.GetTopicID()
		_, ok := pr.p.topics[topic]
		if !ok {
			prune = append(prune, topic)
		} else {
			ePeers := pr.EagerPeers[topic]
			lPeers := pr.LazyPeers[topic]
			if _, ok := ePeers[p]; ok {
				// do nothing
			} else {
				pr.log.Infof("GRAFT: to %s", p.ShortString())
				//eager list does not contain this peer
				ePeers[p] = struct{}{}
				pr.tagPeer(p, topic)
				if _, ok := lPeers[p]; ok {
					delete(lPeers, p)
				}
			}
		}
	}

	if len(prune) == 0 {
		return nil
	}

	cprune := make([]*pb.ControlPrune, 0, len(prune))
	for _, topic := range prune {
		cprune = append(cprune, &pb.ControlPrune{TopicID: &topic})
	}

	return cprune
}

func (pr *PlumtreeRouter) handleIWant(p peer.ID, ctl *pb.ControlMessage) []*pb.Message {
	ihave := make(map[string]*pb.Message)
	for _, iwant := range ctl.GetIwant() {
		for _, mid := range iwant.GetMessageIDs() {
			msg, ok := pr.CacheMessage.Get(mid) //gs.mcache.Get(mid)
			if ok {
				ihave[mid] = msg.(*pb.Message)
			}
		}
	}

	if len(ihave) == 0 {
		return nil
	}

	pr.log.Infof("IWANT: Sending %d messages to %s", len(ihave), p)

	msgs := make([]*pb.Message, 0, len(ihave))
	for _, msg := range ihave {
		msgs = append(msgs, msg)
	}

	return msgs
}

func (pr *PlumtreeRouter) heartbeatTimer() {
	//initial delay
	//select {
	//case pr.p.eval <- pr.heartbeat:
	//case <-pr.p.ctx.Done():
	//	return
	//}
	go func() {
		//initial delay
		time.Sleep(5 * time.Second)
		ticker := time.NewTicker(PlumtreeHeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				pr.heartbeat()
			case <-pr.p.ctx.Done():
				return
			}
		}
	}()
}

//定时将新邻居加入到Lazy Peers中,但是懒得定义一个新的Lazy_Graft消息，加到Eager Peers中也行
func (pr *PlumtreeRouter) heartbeat() {
	pr.log.Debug("plumtree heart beat")
	for _, topic := range pr.p.GetTopics() {
		peers := pr.getPeers(topic, FanoutSize, func(peer.ID) bool {
			return true
		})
		pmap := peerListToMap(peers)
		//pr.EagerPeers[topic] = pmap
		for pid := range pmap {
			_, ok1 := pr.EagerPeers[topic][pid]
			_, ok2 := pr.LazyPeers[topic][pid]
			if !ok1 && !ok2 {
				pr.log.Infof("Heartbeat fix view: Add link to %s in %s", pid, topic)
				pr.EagerPeers[topic][pid] = struct{}{}
				pr.tagPeer(pid, topic)
				pr.sendGraft(pid, topic)
			}
		}
	}
}

// Publish is invoked to forward a new message that has been validated.
func (pr *PlumtreeRouter) Publish(from peer.ID, msg *pb.Message) {
	pr.log.Infof("publish: from=%s, msgid=%s", from.ShortString(), msgID(msg))
	pr.EagerReceivedTime.Add(msgID(msg), time.Now().Unix(), CacheDuration)
	for _, topic := range msg.GetTopicIDs() {
		if pr.p.seenMessages.Has(msgID(msg)) {
			pr.log.Infof("seen msg: msgid=%s", msgID(msg))
			pr.sendPrune(from, topic)
			return
		}
		//message not seen, cache it
		pr.CacheMessage.Add(msgID(msg), msg, CacheDuration)
		tMessage := pb.TransferMessage{}
		err := proto.Unmarshal(msg.Data, &tMessage)
		if err != nil {
			pr.log.Fatal("received an unknown data type message")
			return
		}
		if *tMessage.Type == pb.MessageType_RESPONSE {
			pr.log.Debugf("received response: from=%s msgid=%s", from.ShortString(), msgID(msg))
			rid := tMessage.GetRMsg().GetRequestID()
			if rid == "" {
				pr.log.Warning("invalid RequestID")
			} else {
				pr.getChildrenData(rid)[from] = msg
			}
			return
		}
		pr.cacheInit(msgID(msg), from)
		fatherID, ok := pr.fatherMap.Get(msgID(msg))
		if !ok {
			pr.log.Fatalf("%s node has no father id", pr.p.host.ID().String())
			panic(fmt.Sprintf("%s node has no father id", pr.p.host.ID().String()))
		}
		//感觉应该把等待返回结果的逻辑抽离出来,  但是不在这里做的话，又不知道究竟要等待哪些节点返回的结果
		if fatherID.(peer.ID) != pr.p.host.ID() {
			//如果不是根节点，都需要将消息返回给父节点
			go pr.sendResponseToFather(topic, msgID(msg))
		} else {
			//如果是根节点，将结果返回给查询用户
			pr.TimeCost.Add(msgID(msg), time.Now(), CacheDuration)
			pr.QueryIDCache.Add(tMessage.GetInnerId(), msgID(msg), CacheDuration)
			go pr.sendResponseToUser(topic, msgID(msg))
		}

		var nSteps int32
		nSteps = 0
		if StepsOptimization {
			//increase steps
			tmpMsg := pb.TransferMessage{}
			proto.Unmarshal(msg.Data, &tmpMsg)
			nSteps = *tmpMsg.QMsg.Steps + 1
			*tmpMsg.QMsg.Steps = nSteps
			msg.Signature = nil //不设置为空，会不能广播，因为每次广播都要查看消息是否被修改
			pr.log.Debugf("new steps is : ", *tmpMsg.QMsg.Steps)
			msg.Data, _ = proto.Marshal(&tmpMsg)
		}

		//send message to eager peers
		for pid := range pr.EagerPeers[topic] {
			if pid == from || pid == peer.ID(msg.GetFrom()) {
				continue
			}

			pr.log.Debugf("sending to peer : %s\n", pr.getNoFromID(pid.String()))
			mch, ok := pr.p.peers[pid]
			if !ok {
				continue
			}
			select {
			case mch <- rpcWithMessages(msg):
			default:
				pr.log.Infof("dropping message to peer %s :queue full", pid)
			}
		}
		//send message id to lazy peers
		for pid := range pr.LazyPeers[topic] {
			if pid == from || pid == peer.ID(msg.GetFrom()) {
				continue
			}
			mch, ok := pr.p.peers[pid]
			if !ok {
				continue
			}
			select {
			case mch <- rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{msgID(msg)}, Steps: &nSteps}}, nil, nil, nil):
			default:
				pr.log.Infof("dropping lazy message to peer %s :queue full", pid)
			}
		}
	}
}

//根节点调用
func (pr *PlumtreeRouter) sendResponseToUser(topic string, msgID string) {
	pr.waitMergeFinished(topic, msgID, func() bool {
		return len(pr.getChildrenData(msgID)) == len(pr.EagerPeers[topic])
	})
	resp, err := pr.mergeResponse(msgID)
	if resp == nil {
		// 目前我们并不关心response情况
		pr.log.Debug("nil response: msgid=%s", msgID)
	}
	if err != nil {
		pr.log.Error(err.Error())
	}
	tmp, ok := pr.TimeCost.Get(msgID)
	if !ok {
		pr.log.Errorf("cannot get time cost for %s", msgID)
	}
	pr.log.Warnf("Time cost is: %s\n", time.Now().Sub(tmp.(time.Time)))

	fatherNode, ok := pr.fatherMap.Get(msgID)
	if !ok {
		prompt := "node has no father in cache"
		pr.log.Fatal(prompt)
		panic(prompt)
	}
	if fatherNode.(peer.ID) != pr.p.host.ID() {
		prompt := "current node is not root node"
		pr.log.Error(prompt)
	}
	//把结果加入到 ResultCache 中，方便轮询（感觉可以改成 channel 的形式）
	seqno := pr.p.nextSeqno()
	transMsg := pr.marshalResponse(msgID, resp)
	msg := &pb.Message{
		Data:     transMsg,
		TopicIDs: []string{topic},
		From:     []byte(pr.p.host.ID()),
		Seqno:    seqno,
	}
	err = pr.ResultCache.Add(msgID, msg, CacheDuration)
	if err != nil {
		panic(err)
	}
}

//非根的节点调用
func (pr *PlumtreeRouter) sendResponseToFather(topic string, msgID string) {
	pr.waitMergeFinished(topic, msgID, func() bool {
		return len(pr.getChildrenData(msgID)) == len(pr.EagerPeers[topic])-1
	})
	resp, err := pr.mergeResponse(msgID)
	if resp == nil {
		// 目前我们并不关心response情况，先用debug级别
		pr.log.Debug("nil response: msgid=%s", msgID)
	}
	if err != nil {
		pr.log.Fatal(err.Error())
		return
	}
	seqno := pr.p.nextSeqno()
	responseMsg := &pb.Message{
		//Key在消息返回时，无用处，用作存储查询请求的ID，后期需要定义单独的字段存储查询请求的ID
		Key:      []byte(msgID),
		Data:     pr.marshalResponse(msgID, resp),
		TopicIDs: []string{topic},
		From:     []byte(pr.p.host.ID()),
		Seqno:    seqno,
	}
	fatherID, ok := pr.fatherMap.Get(msgID)
	if !ok {
		pr.log.Fatalf("%s node has no father id\n", pr.p.host.ID().String())
		panic(fmt.Sprintf("%s node has no father id\n", pr.p.host.ID().String()))
	}
	mch, ok := pr.p.peers[fatherID.(peer.ID)]
	if !ok {
		return
	}
	mch <- rpcWithMessages(responseMsg)
	// 这里先别删除缓存，如果超时后返回，还可以利用缓存返回东西
	// pr.cacheClear(msgID)
}

//marshalResponse creates a marshalled response
func (pr *PlumtreeRouter) marshalResponse(msgID string, r Response) []byte {
	responseData := pr.proc.ResponseSerializer(r)
	responseMsg := pb.ResponseMessage{
		RequestID: &msgID,
		Response:  responseData,
	}
	transMsg := pb.TransferMessage{
		Type: pb.MessageType_RESPONSE.Enum(),
		RMsg: &responseMsg,
	}
	bytes, err := proto.Marshal(&transMsg)
	if err != nil {
		pr.log.Error(err.Error())
		pr.log.Error("marshall failed in marshalResponse")
		return nil
	}
	return bytes
}

func (pr *PlumtreeRouter) waitMergeFinished(topic string, msgID string, condition func() bool) {
	go func() {
		fatherNode, ok := pr.fatherMap.Get(msgID)
		if !ok {
			panic(fmt.Sprintf("%s node has no father id\n", pr.p.host.ID().String()))
		}
		father := fatherNode.(peer.ID)
		for {
			if (len(pr.EagerPeers[topic]) == 1 && father != pr.p.host.ID()) || condition() {
				pr.getCollectDone(msgID) <- struct{}{}
				return
			}
			time.Sleep(10 * time.Millisecond)
		} //这里利用轮询来查看是否收到消息，我个人感觉应该可以通过 channel 避免轮询。。。
	}()

	select {
	//感觉只要有一个节点超时，父节点肯定会超时。。。
	case <-time.After(WaitResponseTime):
		pr.log.Infof("time out: %s", msgID)
		return
	case <-pr.getCollectDone(msgID):
		pr.log.Infof("collect done: %s", msgID)
		return
	}
}

func (pr *PlumtreeRouter) getCollectDone(msgID string) chan struct{} {
	tmp, ok := pr.CollectDoneCache.Get(msgID)
	if !ok {
		panic(fmt.Sprintf("have no cache in CollectDoneCache for message : %s", msgID))
	}
	CollectDone := tmp.(chan struct{})
	return CollectDone
}

func (pr *PlumtreeRouter) cacheInit(msgID string, from peer.ID) {
	var err error
	err = pr.fatherMap.Add(msgID, from, CacheDuration)
	if err != nil {
		panic(err)
	}
	err = pr.CollectDoneCache.Add(msgID, make(chan struct{}), CacheDuration)
	if err != nil {
		panic(err)
	}
	err = pr.childrendataCache.Add(msgID, make(map[peer.ID]*pb.Message), CacheDuration)
	if err != nil {
		panic(err)
	}
}

func (pr *PlumtreeRouter) cacheClear(msgID string) {
	pr.fatherMap.Delete(msgID)
	pr.CollectDoneCache.Delete(msgID)
	pr.childrendataCache.Delete(msgID)
}

func (pr *PlumtreeRouter) getChildrenData(msgID string) map[peer.ID]*pb.Message {
	tmp, ok := pr.childrendataCache.Get(msgID)
	if !ok {
		pr.log.Panicf("have no cache in ChildrenData for query message : %s", msgID)
		panic(fmt.Sprintf("have no cache in ChildrenData for query message : %s", msgID))
	}
	return tmp.(map[peer.ID]*pb.Message)
}

func message2Response(msg *pb.Message, proc Processor) (Response, error) {
	data := msg.Data
	transMsg := pb.TransferMessage{}
	err := proto.Unmarshal(data, &transMsg)
	if err != nil || *transMsg.Type != pb.MessageType_RESPONSE {
		log.Error("response message unmarshal error:", err)
		return nil, err
	}
	respStream := transMsg.GetRMsg().GetResponse()
	return proc.ResponseUnserializer(respStream), nil
}

func message2Request(msg *pb.Message, proc Processor) (Request, error) {
	data := msg.Data
	transMsg := pb.TransferMessage{}
	err := proto.Unmarshal(data, &transMsg)
	if err != nil || *transMsg.Type != pb.MessageType_REQUEST {
		log.Error("request message unmarshal error:", err)
		return nil, err
	}
	reqStream := transMsg.GetQMsg().GetRequest()
	return proc.RequestUnserializer(reqStream), nil
}

func (pr *PlumtreeRouter) mergeResponse(msgID string) (Response, error) {
	reqMsg, ok := pr.CacheMessage.Get(msgID)
	if !ok {
		prompt := fmt.Sprintf("cannot get msg cache for %s", msgID)
		pr.log.Fatal(prompt)
		return nil, errors.New(prompt)
	}
	childrenData := pr.getChildrenData(msgID)
	res := make([]Response, 0, len(childrenData)+1)
	req, err := message2Request(reqMsg.(*pb.Message), pr.proc)
	if err != nil {
		return nil, err
	}
	locResp := pr.getLocalResponse(req)
	res = append(res, locResp)
	for _, msg := range childrenData {

		resp, err := message2Response(msg, pr.proc)
		if err != nil {
			continue
		}
		res = append(res, resp)
	}
	return pr.proc.MergeResponseHandler(res), nil
}

//本地处理函数
func (pr *PlumtreeRouter) getLocalResponse(r Request) Response {
	return pr.proc.RequestHandler(r)
}

func (pr *PlumtreeRouter) sendPrune(p peer.ID, topic string) {
	prune := []*pb.ControlPrune{{TopicID: &topic}}
	out := rpcWithControl(nil, nil, nil, nil, prune)
	pr.sendRPC(p, out)
}

func (pr *PlumtreeRouter) sendGraft(p peer.ID, topic string) {
	graft := []*pb.ControlGraft{{TopicID: &topic}}
	out := rpcWithControl(nil, nil, nil, graft, nil)
	pr.sendRPC(p, out)
}

func (pr *PlumtreeRouter) sendRPC(p peer.ID, out *RPC) {
	mch, ok := pr.p.peers[p]
	if !ok {
		return
	}
	select {
	case mch <- out:
	default:
		pr.log.Infof("dropping message to peer %s: queue full", p)
		// push control messages that need to be retried
		//TODO:cache graft, IHAVE to retry
		//ctl := out.GetControl()
		//if ctl != nil {
		//	gs.pushControl(p, ctl)
		//}
	}
}

// Join notifies the router that we want to receive and forward messages in a topic.
// It is invoked after the subscription announcement.
func (pr *PlumtreeRouter) Join(topic string) {
	peers := pr.getPeers(topic, FanoutSize, func(peer.ID) bool {
		return true
	})
	pmap := peerListToMap(peers)
	pr.EagerPeers[topic] = pmap
	for pid := range pmap {
		pr.log.Debugf("JOIN: Add link to %s in %s", pid, topic)
		pr.tagPeer(pid, topic)
		pr.sendGraft(pid, topic)
	}
}

// Leave notifies the router that we are no longer interested in a topic.
// It is invoked after the unsubscription announcement.
func (pr *PlumtreeRouter) Leave(topic string) {}

func (pr *PlumtreeRouter) getPeers(topic string, count int, filter func(peer.ID) bool) []peer.ID {
	tmap, ok := pr.p.topics[topic]
	if !ok {
		return nil
	}

	peers := make([]peer.ID, 0, len(tmap))
	for p := range tmap {
		if filter(p) {
			peers = append(peers, p)
		}
	}

	shufflePeers(peers)

	if count > 0 && len(peers) > count {
		peers = peers[:count]
	}

	return peers
}

func (pr *PlumtreeRouter) tagPeer(p peer.ID, topic string) {
	tag := topicTag(topic)
	pr.p.host.ConnManager().TagPeer(p, tag, 2)
}

func (pr *PlumtreeRouter) untagPeer(p peer.ID, topic string) {
	tag := topicTag(topic)
	pr.p.host.ConnManager().UntagPeer(p, tag)
}

func (pr *PlumtreeRouter) getNoFromID(pid string) string {
	if len(pr.MapID2No) == 0 {
		return pid
	} else {
		return strconv.Itoa(pr.MapID2No[pid])
	}
}

// copied from gossipsub.go
func topicTag(topic string) string {
	return fmt.Sprintf("pubsub:%s", topic)
}

func peerListToMap(peers []peer.ID) map[peer.ID]struct{} {
	pmap := make(map[peer.ID]struct{})
	for _, p := range peers {
		pmap[p] = struct{}{}
	}
	return pmap
}

func peerMapToList(peers map[peer.ID]struct{}) []peer.ID {
	plst := make([]peer.ID, 0, len(peers))
	for p := range peers {
		plst = append(plst, p)
	}
	return plst
}

func shufflePeers(peers []peer.ID) {
	for i := range peers {
		j := rand.Intn(i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}
}

//Request can be in any form
type Request interface{}

//Response can be in any form
type Response interface{}

//Processor deals with Request and Response
type Processor interface {
	//RequestSerializer is invoked before publishing
	RequestSerializer(r Request) []byte
	//RequestUnserializer is invoked when a host receive Request
	RequestUnserializer(stream []byte) Request
	//ResponseSerializer
	ResponseSerializer(r Response) []byte
	//ResponseUnserializer
	ResponseUnserializer(stream []byte) Response
	//RequestHandler deals with the request. Each host with get the same request.
	RequestHandler(r Request) Response
	//MergeResponseHandler tells the fetcher how to merge responses which contains local response. It is invoked when the host received all its children's responses.
	MergeResponseHandler(res []Response) Response
}

//EmptyProcessor used when initialization
type EmptyProcessor struct{}

func (e *EmptyProcessor) RequestSerializer(r Request) []byte {
	return []byte{}
}
func (e *EmptyProcessor) RequestUnserializer(stream []byte) Request {
	return nil
}
func (e *EmptyProcessor) ResponseSerializer(r Response) []byte {
	return []byte{}
}
func (e *EmptyProcessor) ResponseUnserializer(stream []byte) Response {
	return nil
}
func (e *EmptyProcessor) RequestHandler(r Request) Response {
	return nil
}
func (e *EmptyProcessor) MergeResponseHandler(res []Response) Response {
	return nil
}

//SetProcessor after initialization
func (pr *PlumtreeRouter) SetProcessor(p Processor) {
	pr.proc = p
}

//PublishRequest return a channel waiting for response, only for root host
func (pr *PlumtreeRouter) PublishRequest(r Request, topic string) (Response, error) {
	//marshal request
	innerMsgID := generateUUID()
	reqData := pr.proc.RequestSerializer(r)
	reqStep := int32(0)
	qMsg := pb.QueryMessage{
		Steps:   &reqStep,
		Request: reqData,
	}
	transMsg := pb.TransferMessage{
		Type:    pb.MessageType_REQUEST.Enum(),
		InnerId: &innerMsgID,
		QMsg:    &qMsg,
	}
	msg, err := proto.Marshal(&transMsg)
	if err != nil {
		prompt := "publish marshal failed"
		pr.log.Panic(prompt, err.Error())
		panic(prompt)
	}
	err = pr.p.Publish(topic, msg)
	if err != nil {
		pr.log.Error(err.Error())
		return nil, err
	}
	var res Response
	for i := 0; i < 10; i++ {
		//询问10次？等待其他节点返回消息（其他节点怎么处理请求？处理的结果似乎应该写在Router的cache中）
		time.Sleep(200 * time.Millisecond)
		msgID, ok := pr.QueryIDCache.Get(innerMsgID)
		if !ok {
			continue
		}
		//responseMsg 是返回的消息
		responseMsg, ok := pr.ResultCache.Get(msgID.(string))
		if !ok {
			continue
		} else {
			//成功得到结果，不再查看cache
			tr := pb.TransferMessage{}
			err = proto.UnmarshalMerge(responseMsg.(*pb.Message).Data, &tr)
			if err != nil {
				prompt := "unmarshal failed"
				pr.log.Error(prompt)
				return nil, errors.New(prompt)
			}
			res = pr.proc.ResponseUnserializer([]byte(tr.GetRMsg().GetResponse()))
			return res, nil
		}
	}
	prompt := "wait for response time out"
	pr.log.Error(prompt)
	return nil, errors.New(prompt)
}

func generateUUID() string {
	var id string
	temp, err := uuid.NewV4()
	if err != nil {
		log.Fatal("generate uuid failed")
		id = "abcde"
	} else {
		id = temp.String()
	}
	return id
}

type loggerWithID struct {
	*logging.ZapEventLogger
	pid peer.ID
}

func (l *loggerWithID) Debug(args ...interface{}) {
	args = append([]interface{}{l.pid.ShortString(), ": "}, args...)
	l.ZapEventLogger.Debug(args...)
}
func (l *loggerWithID) Debugf(format string, args ...interface{}) {
	args = append([]interface{}{l.pid.ShortString()}, args...)
	l.ZapEventLogger.Debugf("%s: "+format, args...)
}
func (l *loggerWithID) Error(args ...interface{}) {
	args = append([]interface{}{l.pid.ShortString(), ": "}, args...)
	l.ZapEventLogger.Error(args...)
}
func (l *loggerWithID) Errorf(format string, args ...interface{}) {
	args = append([]interface{}{l.pid.ShortString()}, args...)
	l.ZapEventLogger.Errorf("%s: "+format, args...)
}
func (l *loggerWithID) Fatal(args ...interface{}) {
	args = append([]interface{}{l.pid.ShortString(), ": "}, args...)
	l.ZapEventLogger.Fatal(args...)
}
func (l *loggerWithID) Fatalf(format string, args ...interface{}) {
	args = append([]interface{}{l.pid.ShortString()}, args...)
	l.ZapEventLogger.Fatalf("%s: "+format, args...)
}
func (l *loggerWithID) Info(args ...interface{}) {
	args = append([]interface{}{l.pid.ShortString(), ": "}, args...)
	l.ZapEventLogger.Info(args...)
}
func (l *loggerWithID) Infof(format string, args ...interface{}) {
	args = append([]interface{}{l.pid.ShortString()}, args...)
	l.ZapEventLogger.Infof("%s: "+format, args...)
}

func (l *loggerWithID) Panic(args ...interface{}) {
	args = append([]interface{}{l.pid.ShortString(), ": "}, args...)
	l.ZapEventLogger.Panic(args...)
}
func (l *loggerWithID) Panicf(format string, args ...interface{}) {
	args = append([]interface{}{l.pid.ShortString()}, args...)
	l.ZapEventLogger.Panicf("%s: "+format, args...)
}
func (l *loggerWithID) Warn(args ...interface{}) {
	args = append([]interface{}{l.pid.ShortString(), ": "}, args...)
	l.ZapEventLogger.Warn(args...)
}
func (l *loggerWithID) Warnf(format string, args ...interface{}) {
	args = append([]interface{}{l.pid.ShortString()}, args...)
	l.ZapEventLogger.Warnf("%s: "+format, args...)
}
