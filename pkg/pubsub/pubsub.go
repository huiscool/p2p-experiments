package pubsub

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	pb "github.com/huiscool/p2p-experiments/pkg/pubsub/pb"

	logging "github.com/ipfs/go-log"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	host "github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	inet "github.com/libp2p/go-libp2p-net"
	"github.com/whyrusleeping/timecache"
)

const (
	defaultValidateTimeout     = 150 * time.Millisecond
	defaultValidateConcurrency = 100
	defaultValidateThrottle    = 8192
)

var (
	TimeCacheDuration = 120 * time.Second
)

var log = logging.Logger("pubsub")

// PubSub is the implementation of the pubsub system.
type PubSub struct {
	// atomic counter for seqnos
	// NOTE: Must be declared at the top of the struct as we perform atomic
	// operations on this field.
	//
	// See: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	counter uint64

	host host.Host

	rt PubSubRouter

	// incoming messages from other peers
	incoming chan *RPC

	// messages we are publishing out to our peers
	publish chan *Message

	// addSub is a control channel for us to add and remove subscriptions
	addSub chan *addSubReq

	// get list of topics we are subscribed to
	getTopics chan *topicReq

	// get chan of peers we are connected to
	getPeers chan *listPeerReq

	// send subscription here to cancel it
	cancelCh chan *Subscription

	// a notification channel for new peer connections
	newPeers chan peer.ID

	// a notification channel for new outoging peer streams
	newPeerStream chan inet.Stream

	// a notification channel for errors opening new peer streams
	newPeerError chan peer.ID

	// a notification channel for when our peers die
	peerDead chan peer.ID

	// The set of topics we are subscribed to
	myTopics map[string]map[*Subscription]struct{}

	// topics tracks which topics each of our peers are subscribed to
	topics map[string]map[peer.ID]struct{}

	// sendMsg handles messages that have been validated
	sendMsg chan *sendReq

	// addVal handles validator registration requests
	addVal chan *addValReq

	// rmVal handles validator unregistration requests
	rmVal chan *rmValReq

	// topicVals tracks per topic validators
	topicVals map[string]*topicVal

	// validateThrottle limits the number of active validation goroutines
	validateThrottle chan struct{}

	// eval thunk in event loop
	eval chan func()

	// peer blacklist
	blacklist     Blacklist
	blacklistPeer chan peer.ID

	peers        map[peer.ID]chan *RPC
	seenMessages *timecache.TimeCache

	// key for signing messages; nil when signing is disabled (default for now)
	signKey crypto.PrivKey
	// source ID for signed messages; corresponds to signKey
	signID peer.ID
	// strict mode rejects all unsigned messages prior to validation
	signStrict bool

	ctx context.Context
}

// PubSubRouter is the message router component of PubSub.
type PubSubRouter interface {
	// Protocols returns the list of protocols supported by the router.
	Protocols() []protocol.ID
	// Attach is invoked by the PubSub constructor to attach the router to a
	// freshly initialized PubSub instance.
	Attach(*PubSub)
	// AddPeer notifies the router that a new peer has been connected.
	AddPeer(peer.ID, protocol.ID)
	// RemovePeer notifies the router that a peer has been disconnected.
	RemovePeer(peer.ID)
	// HandleRPC is invoked to process control messages in the RPC envelope.
	// It is invoked after subscriptions and payload messages have been processed.
	HandleRPC(*RPC)
	// Publish is invoked to forward a new message that has been validated.
	Publish(peer.ID, *pb.Message)
	// Join notifies the router that we want to receive and forward messages in a topic.
	// It is invoked after the subscription announcement.
	Join(topic string)
	// Leave notifies the router that we are no longer interested in a topic.
	// It is invoked after the unsubscription announcement.
	Leave(topic string)
}

type Message struct {
	*pb.Message
}

func (m *Message) GetFrom() peer.ID {
	return peer.ID(m.Message.GetFrom())
}

type RPC struct {
	pb.RPC

	// unexported on purpose, not sending this over the wire
	from peer.ID
}

type Option func(*PubSub) error

// NewPubSub returns a new PubSub management object.
func NewPubSub(ctx context.Context, h host.Host, rt PubSubRouter, opts ...Option) (*PubSub, error) {
	ps := &PubSub{
		host:             h,
		ctx:              ctx,
		rt:               rt,
		signID:           h.ID(),
		signKey:          h.Peerstore().PrivKey(h.ID()),
		incoming:         make(chan *RPC, 32),
		publish:          make(chan *Message),
		newPeers:         make(chan peer.ID),
		newPeerStream:    make(chan inet.Stream),
		newPeerError:     make(chan peer.ID),
		peerDead:         make(chan peer.ID),
		cancelCh:         make(chan *Subscription),
		getPeers:         make(chan *listPeerReq),
		addSub:           make(chan *addSubReq),
		getTopics:        make(chan *topicReq),
		sendMsg:          make(chan *sendReq, 32),
		addVal:           make(chan *addValReq),
		rmVal:            make(chan *rmValReq),
		validateThrottle: make(chan struct{}, defaultValidateThrottle),
		eval:             make(chan func()),
		myTopics:         make(map[string]map[*Subscription]struct{}),
		topics:           make(map[string]map[peer.ID]struct{}),
		peers:            make(map[peer.ID]chan *RPC),
		topicVals:        make(map[string]*topicVal),
		blacklist:        NewMapBlacklist(),
		blacklistPeer:    make(chan peer.ID),
		seenMessages:     timecache.NewTimeCache(TimeCacheDuration),
		counter:          uint64(time.Now().UnixNano()),
	}

	for _, opt := range opts {
		err := opt(ps)
		if err != nil {
			return nil, err
		}
	}

	if ps.signStrict && ps.signKey == nil {
		return nil, fmt.Errorf("strict signature verification enabled but message signing is disabled")
	}

	rt.Attach(ps)

	for _, id := range rt.Protocols() {
		h.SetStreamHandler(id, ps.handleNewStream)
	}
	h.Network().Notify((*PubSubNotif)(ps))

	go ps.processLoop(ctx)

	return ps, nil
}

// WithValidateThrottle sets the upper bound on the number of active validation
// goroutines.
func WithValidateThrottle(n int) Option {
	return func(ps *PubSub) error {
		ps.validateThrottle = make(chan struct{}, n)
		return nil
	}
}

// WithMessageSigning enables or disables message signing (enabled by default).
func WithMessageSigning(enabled bool) Option {
	return func(p *PubSub) error {
		if enabled {
			p.signKey = p.host.Peerstore().PrivKey(p.signID)
			if p.signKey == nil {
				return fmt.Errorf("can't sign for peer %s: no private key", p.signID)
			}
		} else {
			p.signKey = nil
		}
		return nil
	}
}

// WithMessageAuthor sets the author for outbound messages to the given peer ID
// (defaults to the host's ID). If message signing is enabled, the private key
// must be available in the host's peerstore.
func WithMessageAuthor(author peer.ID) Option {
	return func(p *PubSub) error {
		if author == "" {
			author = p.host.ID()
		}
		if p.signKey != nil {
			newSignKey := p.host.Peerstore().PrivKey(author)
			if newSignKey == nil {
				return fmt.Errorf("can't sign for peer %s: no private key", p.signID)
			}
			p.signKey = newSignKey
		}
		p.signID = author
		return nil
	}
}

// WithStrictSignatureVerification enforces message signing. If set, unsigned
// messages will be discarded.
//
// This currently defaults to false but, as we transition to signing by default,
// will eventually default to true.
func WithStrictSignatureVerification(required bool) Option {
	return func(p *PubSub) error {
		p.signStrict = required
		return nil
	}
}

// WithBlacklist provides an implementation of the blacklist; the default is a
// MapBlacklist
func WithBlacklist(b Blacklist) Option {
	return func(p *PubSub) error {
		p.blacklist = b
		return nil
	}
}

// processLoop handles all inputs arriving on the channels
func (p *PubSub) processLoop(ctx context.Context) {
	defer func() {
		// Clean up go routines.
		for _, ch := range p.peers {
			close(ch)
		}
		p.peers = nil
		p.topics = nil
	}()

	for {
		select {
		//条件：只要新节点调用了 connect, 并且连接到host, 就会被传到这里？
		//动作：在本地为它创建消息的通道，并发送一个 helloPackage
		case pid := <-p.newPeers:
			if _, ok := p.peers[pid]; ok {
				log.Warning("already have connection to peer: ", pid)
				continue
			}

			if p.blacklist.Contains(pid) {
				log.Warning("ignoring connection from blacklisted peer: ", pid)
				continue
			}

			messages := make(chan *RPC, 32)
			messages <- p.getHelloPacket()
			go p.handleNewPeer(ctx, pid, messages)
			//创建一个 *RPC 的 channel， 到时只需要知道对方的 pid，向这个 channel 里传，就能发消息给这个节点了
			//在 handleNewPeer里面，创建了handleSendingMessage 和 handleEOF 两个 goroutine, 当向 messages 这个 channel 里面写东西时，就相当于向新建的 stream 里面写东西
			p.peers[pid] = messages
		//条件：newPeerStream 当新节点加入后，需要创建一个 stream , stream 创建成功，把成功创建的 newPeerStream 通过这个管道进入消息循环
		//动作：如果不在黑名单，向 router 通知加入新的节点
		case s := <-p.newPeerStream:
			pid := s.Conn().RemotePeer()

			ch, ok := p.peers[pid]
			if !ok {
				log.Warning("new stream for unknown peer: ", pid)
				s.Reset()
				continue
			}

			if p.blacklist.Contains(pid) {
				log.Warning("closing stream for blacklisted peer: ", pid)
				close(ch)
				s.Reset()
				continue
			}

			p.rt.AddPeer(pid, s.Protocol())
		//条件：handleNewPeer 中，出现 ErrorNotSupported
		//动作：删除链接
		case pid := <-p.newPeerError:
			delete(p.peers, pid)
		//条件：1.handleNewPeer中，出现链接错误；2.handlePeerEOF 中出错？
		//动作：如果还连着，重新发送 helloPacket; 确保 p.peers[pid]已经关掉，关掉后通知router
		case pid := <-p.peerDead:
			ch, ok := p.peers[pid]
			if !ok {
				continue
			}

			close(ch)

			if p.host.Network().Connectedness(pid) == inet.Connected {
				// still connected, must be a duplicate connection being closed.
				// we respawn the writer as we need to ensure there is a stream active
				log.Warning("peer declared dead but still connected; respawning writer: ", pid)
				messages := make(chan *RPC, 32)
				messages <- p.getHelloPacket()
				go p.handleNewPeer(ctx, pid, messages)
				p.peers[pid] = messages
				continue
			}

			delete(p.peers, pid)
			for _, t := range p.topics {
				delete(t, pid)
			}

			p.rt.RemovePeer(pid)
		//条件：用户调用 GetTopic()方法
		//动作：把 mytopics 写回 response channel
		//当调用 GetTopics 方法时，向 getTopics 传入一个包装好的 topicRequest在这个 treq 里面有一个resp 的 channel ，然后在 loop 里面把 myTopics 里面的东西倒到 resp 这个里面 （为何如此麻烦？我的想法是为了保证 myTopics 的原子性吧，如果直接在外面调，有可能读到不完整的 string）
		case treq := <-p.getTopics:
			var out []string
			for t := range p.myTopics {
				out = append(out, t)
			}
			treq.resp <- out
		//条件： 调用 sub.Cancel()
		//动作：
		// handleRemoveSubscription removes Subscription sub from bookeeping.
		// If this was the last Subscription for a given topic, it will also announce
		// that this node is not subscribing to this topic anymore.
		case sub := <-p.cancelCh:
			p.handleRemoveSubscription(sub)
		// 条件：调用 p.Subscribe 方法
		// handleAddSubscription adds a Subscription for a particular topic. If it is
		// the first Subscription for the topic, it will announce that this node
		// subscribes to the topic.
		case sub := <-p.addSub:
			p.handleAddSubscription(sub)
		//条件：调用 p.GetPeers 方法
		//动作：返回订阅了某个 topic 的 peers 列表
		case preq := <-p.getPeers:
			tmap, ok := p.topics[preq.topic]
			if preq.topic != "" && !ok {
				preq.resp <- nil
				continue
			}
			var peers []peer.ID
			for p := range p.peers {
				if preq.topic != "" {
					_, ok := tmap[p]
					if !ok {
						continue
					}
				}
				peers = append(peers, p)
			}
			preq.resp <- peers
		//条件：外面的stream 设置的 streamHandler 就是 handleNewStream，由 handleNewStream 传入
		//动作：1. 读取 RPC 中的 subscriptions 字段，判断是不是别的节点在 announce 自己的 subscribe，是的话，加入到自己的 topics[topic] 集合中, 或者删除自己的 topics[topic]集合（感觉这里有点隐患，如果一个恶意节点假装自己是最后一个节点，甚至可以删除其他节点的 subscriptions 集合)
		// 2. 读取 RPC中的 publish 字段，如果不感兴趣，直接扔掉；否则，验证之后，我们将进行转发(调用 pushMsg)
		// 3. 直接交给 router 调用 HandleRPC
		// 从 rpc 里面可以读出 from 字段，也就是从谁发过来的。
		case rpc := <-p.incoming:
			p.handleIncomingRPC(rpc)
		//条件：用户调用 Publish 方法
		//动作：验证一下之后通过 pushMsg 转发，会调用 publishMessage
		case msg := <-p.publish:
			vals := p.getValidators(msg)
			p.pushMsg(vals, p.host.ID(), msg)
		//条件：在 validate 方法中，会向 sendMsg 方法中写东西
		// 搞这么麻烦，是希望把 validate 做成异步操作
		//动作：publishMessage ：
		// 1. 如果消息已经看到过，返回；
		// 2. 如果没看过，标记为已看，通过 router.Publish 转发
		case req := <-p.sendMsg:
			p.publishMessage(req.from, req.msg.Message)

		case req := <-p.addVal:
			p.addValidator(req)

		case req := <-p.rmVal:
			p.rmValidator(req)

		case thunk := <-p.eval:
			thunk()

		case pid := <-p.blacklistPeer:
			log.Infof("Blacklisting peer %s", pid)
			p.blacklist.Add(pid)

			ch, ok := p.peers[pid]
			if ok {
				close(ch)
				delete(p.peers, pid)
				for _, t := range p.topics {
					delete(t, pid)
				}
				p.rt.RemovePeer(pid)
			}

		case <-ctx.Done():
			log.Info("pubsub processloop shutting down")
			return
		}
	}
}

// handleRemoveSubscription removes Subscription sub from bookeeping.
// If this was the last Subscription for a given topic, it will also announce
// that this node is not subscribing to this topic anymore.
// Only called from processLoop.
func (p *PubSub) handleRemoveSubscription(sub *Subscription) {
	subs := p.myTopics[sub.topic]

	if subs == nil {
		return
	}

	sub.err = fmt.Errorf("subscription cancelled by calling sub.Cancel()")
	close(sub.ch)
	delete(subs, sub)

	if len(subs) == 0 {
		delete(p.myTopics, sub.topic)
		p.announce(sub.topic, false)
		p.rt.Leave(sub.topic)
	}
}

// handleAddSubscription adds a Subscription for a particular topic. If it is
// the first Subscription for the topic, it will announce that this node
// subscribes to the topic.
// Only called from processLoop.
func (p *PubSub) handleAddSubscription(req *addSubReq) {
	sub := req.sub
	subs := p.myTopics[sub.topic]

	// announce we want this topic
	if len(subs) == 0 {
		p.announce(sub.topic, true)
		p.rt.Join(sub.topic)
	}

	// make new if not there
	if subs == nil {
		p.myTopics[sub.topic] = make(map[*Subscription]struct{})
		subs = p.myTopics[sub.topic]
	}

	sub.ch = make(chan *Message, 32)
	sub.cancelCh = p.cancelCh

	p.myTopics[sub.topic][sub] = struct{}{}

	req.resp <- sub
}

// announce announces whether or not this node is interested in a given topic
// Only called from processLoop.
func (p *PubSub) announce(topic string, sub bool) {
	subopt := &pb.RPC_SubOpts{
		Topicid:   &topic,
		Subscribe: &sub,
	}

	out := rpcWithSubs(subopt)
	for pid, peer := range p.peers {
		select {
		case peer <- out:
		default:
			log.Infof("Can't send announce message to peer %s: queue full; scheduling retry", pid)
			go p.announceRetry(pid, topic, sub)
		}
	}
}

func (p *PubSub) announceRetry(pid peer.ID, topic string, sub bool) {
	time.Sleep(time.Duration(1+rand.Intn(1000)) * time.Millisecond)

	retry := func() {
		_, ok := p.myTopics[topic]
		if (ok && sub) || (!ok && !sub) {
			p.doAnnounceRetry(pid, topic, sub)
		}
	}

	select {
	case p.eval <- retry:
	case <-p.ctx.Done():
	}
}

func (p *PubSub) doAnnounceRetry(pid peer.ID, topic string, sub bool) {
	peer, ok := p.peers[pid]
	if !ok {
		return
	}

	subopt := &pb.RPC_SubOpts{
		Topicid:   &topic,
		Subscribe: &sub,
	}

	out := rpcWithSubs(subopt)
	select {
	case peer <- out:
	default:
		log.Infof("Can't send announce message to peer %s: queue full; scheduling retry", pid)
		go p.announceRetry(pid, topic, sub)
	}
}

// notifySubs sends a given message to all corresponding subscribers.
// Only called from processLoop.
func (p *PubSub) notifySubs(msg *pb.Message) {
	for _, topic := range msg.GetTopicIDs() {
		subs := p.myTopics[topic]
		for f := range subs {
			select {
			case f.ch <- &Message{msg}:
			default:
				log.Infof("Can't deliver message to subscription for topic %s; subscriber too slow", topic)
			}
		}
	}
}

// seenMessage returns whether we already saw this message before
func (p *PubSub) seenMessage(id string) bool {
	//return p.seenMessages.Has(id)
	//PubsubRouter中的Publish中，需要根据是否收到，来发送控制信息。故留到后面的Publish方法，在做判断
	if p.hasPlumTreeProtocol() {
		return false
	}
	return p.seenMessages.Has(id)
}

func (p *PubSub) hasPlumTreeProtocol() bool {
	for _, prot := range p.rt.Protocols() {
		if prot == PlumtreeSubID {
			return true
		}
	}
	return false
}

// markSeen marks a message as seen such that seenMessage returns `true' for the given id
func (p *PubSub) markSeen(id string) {
	if !p.seenMessages.Has(id) {
		p.seenMessages.Add(id)
	}
}

// subscribedToMessage returns whether we are subscribed to one of the topics
// of a given message
func (p *PubSub) subscribedToMsg(msg *pb.Message) bool {
	if len(p.myTopics) == 0 {
		return false
	}

	for _, t := range msg.GetTopicIDs() {
		if _, ok := p.myTopics[t]; ok {
			return true
		}
	}
	return false
}

func (p *PubSub) handleIncomingRPC(rpc *RPC) {
	for _, subopt := range rpc.GetSubscriptions() {
		t := subopt.GetTopicid()
		if subopt.GetSubscribe() {
			tmap, ok := p.topics[t]
			if !ok {
				tmap = make(map[peer.ID]struct{})
				p.topics[t] = tmap
			}

			tmap[rpc.from] = struct{}{}
		} else {
			tmap, ok := p.topics[t]
			if !ok {
				continue
			}
			delete(tmap, rpc.from)
		}
	}

	for _, pmsg := range rpc.GetPublish() {
		if !p.subscribedToMsg(pmsg) {
			log.Warning("received message we didn't subscribe to. Dropping.")
			continue
		}

		msg := &Message{pmsg}
		vals := p.getValidators(msg)
		p.pushMsg(vals, rpc.from, msg)
	}

	p.rt.HandleRPC(rpc)
}

// msgID returns a unique ID of the passed Message
func msgID(pmsg *pb.Message) string {
	from := pmsg.GetFrom()
	seqno := pmsg.GetSeqno()
	var bin []byte = make([]byte, 0, len(from)+len(seqno))
	bin = append(bin, from...)
	bin = append(bin, seqno...)
	return hex.EncodeToString(bin)
}

// pushMsg pushes a message performing validation as necessary
func (p *PubSub) pushMsg(vals []*topicVal, src peer.ID, msg *Message) {
	// reject messages from blacklisted peers
	if p.blacklist.Contains(src) {
		log.Warningf("dropping message from blacklisted peer %s", src)
		return
	}

	// even if they are forwarded by good peers
	if p.blacklist.Contains(msg.GetFrom()) {
		log.Warningf("dropping message from blacklisted source %s", src)
		return
	}

	// reject unsigned messages when strict before we even process the id
	if p.signStrict && msg.Signature == nil {
		log.Debugf("dropping unsigned message from %s", src)
		return
	}

	// have we already seen and validated this message?
	id := msgID(msg.Message)
	if p.seenMessage(id) {
		return
	}

	if len(vals) > 0 || msg.Signature != nil {
		// validation is asynchronous and globally throttled with the throttleValidate semaphore.
		// the purpose of the global throttle is to bound the goncurrency possible from incoming
		// network traffic; each validator also has an individual throttle to preclude
		// slow (or faulty) validators from starving other topics; see validate below.
		select {
		case p.validateThrottle <- struct{}{}:
			go func() {
				p.validate(vals, src, msg)
				<-p.validateThrottle
			}()
		default:
			log.Warningf("message validation throttled; dropping message from %s", src)
		}
		return
	}

	p.publishMessage(src, msg.Message)
}

// validate performs validation and only sends the message if all validators succeed
func (p *PubSub) validate(vals []*topicVal, src peer.ID, msg *Message) {
	if msg.Signature != nil {
		if !p.validateSignature(msg) {
			log.Warningf("message signature validation failed; dropping message from %s", src)
			return
		}
	}

	if len(vals) > 0 {
		if !p.validateTopic(vals, src, msg) {
			log.Warningf("message validation failed; dropping message from %s", src)
			return
		}
	}

	// all validators were successful, send the message
	p.sendMsg <- &sendReq{
		from: src,
		msg:  msg,
	}
}

func (p *PubSub) validateSignature(msg *Message) bool {
	err := verifyMessageSignature(msg.Message)
	if err != nil {
		log.Debugf("signature verification error: %s", err.Error())
		return false
	}

	return true
}

func (p *PubSub) validateTopic(vals []*topicVal, src peer.ID, msg *Message) bool {
	if len(vals) == 1 {
		return p.validateSingleTopic(vals[0], src, msg)
	}

	ctx, cancel := context.WithCancel(p.ctx)
	defer cancel()

	rch := make(chan bool, len(vals))
	rcount := 0
	throttle := false

loop:
	for _, val := range vals {
		rcount++

		select {
		case val.validateThrottle <- struct{}{}:
			go func(val *topicVal) {
				rch <- val.validateMsg(ctx, src, msg)
				<-val.validateThrottle
			}(val)

		default:
			log.Debugf("validation throttled for topic %s", val.topic)
			throttle = true
			break loop
		}
	}

	if throttle {
		return false
	}

	for i := 0; i < rcount; i++ {
		valid := <-rch
		if !valid {
			return false
		}
	}

	return true
}

// fast path for single topic validation that avoids the extra goroutine
func (p *PubSub) validateSingleTopic(val *topicVal, src peer.ID, msg *Message) bool {
	select {
	case val.validateThrottle <- struct{}{}:
		ctx, cancel := context.WithCancel(p.ctx)
		defer cancel()

		res := val.validateMsg(ctx, src, msg)
		<-val.validateThrottle

		return res

	default:
		log.Debugf("validation throttled for topic %s", val.topic)
		return false
	}
}

func (p *PubSub) publishMessage(from peer.ID, pmsg *pb.Message) {
	id := msgID(pmsg)
	if p.seenMessage(id) {
		return
	}
	//p.markSeen(id)
	if !p.seenMessages.Has(id) && !p.hasPlumTreeProtocol() {
		// Plumtree 暂不提供给外部使用全网广播消息功能。只用作查询
		p.notifySubs(pmsg)
	}
	p.rt.Publish(from, pmsg)
	p.markSeen(id)
}

// getValidators returns all validators that apply to a given message
func (p *PubSub) getValidators(msg *Message) []*topicVal {
	var vals []*topicVal

	for _, topic := range msg.GetTopicIDs() {
		val, ok := p.topicVals[topic]
		if !ok {
			continue
		}

		vals = append(vals, val)
	}

	return vals
}

type addSubReq struct {
	sub  *Subscription
	resp chan *Subscription
}

type SubOpt func(sub *Subscription) error

// Subscribe returns a new Subscription for the given topic.
// Note that subscription is not an instanteneous operation. It may take some time
// before the subscription is processed by the pubsub main loop and propagated to our peers.
func (p *PubSub) Subscribe(topic string, opts ...SubOpt) (*Subscription, error) {
	td := pb.TopicDescriptor{Name: &topic}

	return p.SubscribeByTopicDescriptor(&td, opts...)
}

// SubscribeByTopicDescriptor lets you subscribe a topic using a pb.TopicDescriptor.
func (p *PubSub) SubscribeByTopicDescriptor(td *pb.TopicDescriptor, opts ...SubOpt) (*Subscription, error) {
	if td.GetAuth().GetMode() != pb.TopicDescriptor_AuthOpts_NONE {
		return nil, fmt.Errorf("auth mode not yet supported")
	}

	if td.GetEnc().GetMode() != pb.TopicDescriptor_EncOpts_NONE {
		return nil, fmt.Errorf("encryption mode not yet supported")
	}

	sub := &Subscription{
		topic: td.GetName(),
	}

	for _, opt := range opts {
		err := opt(sub)
		if err != nil {
			return nil, err
		}
	}

	out := make(chan *Subscription, 1)
	p.addSub <- &addSubReq{
		sub:  sub,
		resp: out,
	}

	return <-out, nil
}

type topicReq struct {
	resp chan []string
}

// GetTopics returns the topics this node is subscribed to.
func (p *PubSub) GetTopics() []string {
	out := make(chan []string, 1)
	p.getTopics <- &topicReq{resp: out}
	return <-out
}

// Publish publishes data to the given topic.
func (p *PubSub) Publish(topic string, data []byte) error {
	seqno := p.nextSeqno()
	m := &pb.Message{
		Data:     data,
		TopicIDs: []string{topic},
		From:     []byte(p.host.ID()),
		Seqno:    seqno,
	}
	if p.signKey != nil {
		m.From = []byte(p.signID)
		err := signMessage(p.signID, p.signKey, m)
		if err != nil {
			return err
		}
	}
	p.publish <- &Message{m}
	return nil
}

func (p *PubSub) nextSeqno() []byte {
	seqno := make([]byte, 8)
	counter := atomic.AddUint64(&p.counter, 1)
	binary.BigEndian.PutUint64(seqno, counter)
	return seqno
}

type listPeerReq struct {
	resp  chan []peer.ID
	topic string
}

// sendReq is a request to call publishMessage.
// It is issued after message validation is done.
type sendReq struct {
	from peer.ID
	msg  *Message
}

// ListPeers returns a list of peers we are connected to in the given topic.
func (p *PubSub) ListPeers(topic string) []peer.ID {
	out := make(chan []peer.ID)
	p.getPeers <- &listPeerReq{
		resp:  out,
		topic: topic,
	}
	return <-out
}

// BlacklistPeer blacklists a peer; all messages from this peer will be unconditionally dropped.
func (p *PubSub) BlacklistPeer(pid peer.ID) {
	p.blacklistPeer <- pid
}

// per topic validators
type addValReq struct {
	topic    string
	validate Validator
	timeout  time.Duration
	throttle int
	resp     chan error
}

type rmValReq struct {
	topic string
	resp  chan error
}

type topicVal struct {
	topic            string
	validate         Validator
	validateTimeout  time.Duration
	validateThrottle chan struct{}
}

// Validator is a function that validates a message.
type Validator func(context.Context, peer.ID, *Message) bool

// ValidatorOpt is an option for RegisterTopicValidator.
type ValidatorOpt func(addVal *addValReq) error

// WithValidatorTimeout is an option that sets the topic validator timeout.
func WithValidatorTimeout(timeout time.Duration) ValidatorOpt {
	return func(addVal *addValReq) error {
		addVal.timeout = timeout
		return nil
	}
}

// WithValidatorConcurrency is an option that sets topic validator throttle.
func WithValidatorConcurrency(n int) ValidatorOpt {
	return func(addVal *addValReq) error {
		addVal.throttle = n
		return nil
	}
}

// RegisterTopicValidator registers a validator for topic.
func (p *PubSub) RegisterTopicValidator(topic string, val Validator, opts ...ValidatorOpt) error {
	addVal := &addValReq{
		topic:    topic,
		validate: val,
		resp:     make(chan error, 1),
	}

	for _, opt := range opts {
		err := opt(addVal)
		if err != nil {
			return err
		}
	}

	p.addVal <- addVal
	return <-addVal.resp
}

func (ps *PubSub) addValidator(req *addValReq) {
	topic := req.topic

	_, ok := ps.topicVals[topic]
	if ok {
		req.resp <- fmt.Errorf("Duplicate validator for topic %s", topic)
		return
	}

	val := &topicVal{
		topic:            topic,
		validate:         req.validate,
		validateTimeout:  defaultValidateTimeout,
		validateThrottle: make(chan struct{}, defaultValidateConcurrency),
	}

	if req.timeout > 0 {
		val.validateTimeout = req.timeout
	}

	if req.throttle > 0 {
		val.validateThrottle = make(chan struct{}, req.throttle)
	}

	ps.topicVals[topic] = val
	req.resp <- nil
}

// UnregisterTopicValidator removes a validator from a topic.
// Returns an error if there was no validator registered with the topic.
func (p *PubSub) UnregisterTopicValidator(topic string) error {
	rmVal := &rmValReq{
		topic: topic,
		resp:  make(chan error, 1),
	}

	p.rmVal <- rmVal
	return <-rmVal.resp
}

func (ps *PubSub) rmValidator(req *rmValReq) {
	topic := req.topic

	_, ok := ps.topicVals[topic]
	if ok {
		delete(ps.topicVals, topic)
		req.resp <- nil
	} else {
		req.resp <- fmt.Errorf("No validator for topic %s", topic)
	}
}

func (val *topicVal) validateMsg(ctx context.Context, src peer.ID, msg *Message) bool {
	vctx, cancel := context.WithTimeout(ctx, val.validateTimeout)
	defer cancel()

	valid := val.validate(vctx, src, msg)
	if !valid {
		log.Debugf("validation failed for topic %s", val.topic)
	}

	return valid
}
