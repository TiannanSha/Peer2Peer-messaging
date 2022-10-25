package impl

import (
	"encoding/json"
	"errors"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"sync"
	"time"
)

// @Author: Tiannan Sha
// @Email: tiannan.sha@epfl.ch

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.
	routingTable := make(map[string]string)
	routingTable[conf.Socket.GetAddress()]= conf.Socket.GetAddress()
	stopSig := make(chan bool, 1)
	statusMsg := make(map[string]uint)
	rumorsReceived := make(map[string][]types.Rumor)
	Status := status{StatusMsg: statusMsg, rumorsReceived: rumorsReceived}
	nbrsMap := make(map[string]bool)
	nbrs := nbrSet{nbrs: nbrsMap}
	channelsMap := make(map[string]chan bool)
	pktAckChannels := chanPool{pktAckChannels: channelsMap}


	return &node{conf:conf, routingTable: routingTable, stopSigCh: stopSig, Status: &Status,
		addr: conf.Socket.GetAddress(), nbrSet: &nbrs, pktAckChannels: &pktAckChannels}
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	// You probably want to keep the peer.Configuration on this struct:
	conf peer.Configuration
	stopSigCh chan bool
	routingTable peer.RoutingTable
	sync.RWMutex
	//sequenceNumber uint // sequence number of last created
	Status *status
	nbrSet *nbrSet
	addr   string
	antiEntropyQuitCh chan struct{} // initialized when starting antiEntropy mechanism
	heartbeatQuitCh chan struct{} // initialized when starting heartbeat mechanism
	//rumorsReceived map[string][]types.Rumor
	pktAckChannels *chanPool
	//rwmutexPktAckChannels sync.RWMutex
}

// Start implements peer.Service
// peer only can control recv() from socket, doesn't have access to create and close socket, those seem to be the job
// of transport layer (?)
func (n *node) Start() error {
	//panic("to be implemented in HW0")
	// add my addr to the routing table
	// n.AddPeer(n.conf.Socket.GetAddress())
	log.Info().Msgf("in node Start()***************, node: %s", n.addr)
	//  register handlers for different types of messages
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.ExecChatMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, n.ExecRumorsMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, n.ExecAckMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, n.ExecStatusMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, n.ExecPrivateMessage)

	// optionally start anti-entropy mechanism
	if (n.conf.AntiEntropyInterval>0) {
		n.startAntiEntropy()
	}

	// optionally start heartbeat mechanism
	if (n.conf.HeartbeatInterval>0) {
		n.startHeartbeat()
	}

	go func() {
		for {
			select {
			case <- n.stopSigCh:
				return
			default:
				pkt, err := n.conf.Socket.Recv(time.Second * 1) //was 2 somehow
				if errors.Is(err, transport.TimeoutError(0)) {
					//log.Info().Msg("in start(), timeout")
					continue
				} else {
					// process packet if packet dest shows it's for me, otherwise relay to the actual dest
					n.handlePacket(pkt)
				}
			}
		}
	}()
	return nil
}

// process packet if dest is my addr, otherwise forward the packet to the actual dest
func (n* node) handlePacket(pkt transport.Packet) {
	log.Info().Msgf("node %s handlePacket()", n.addr)
	log.Info().Msg("pkt:")
	log.Info().Msgf("%s", pkt)
	log.Info().Msg("----------------------------------")
	mySocketAddr := n.conf.Socket.GetAddress()
	if (pkt.Header.Destination == n.conf.Socket.GetAddress() ) {
		//log.Debug().Msgf("node %s, pkt: %s", pkt, n.addr)
		err := n.conf.MessageRegistry.ProcessPacket(pkt)
		if (err!=nil) {
			log.Warn().Msgf("node %s, in handlePacket(),err:%v",n.addr, err)
		}
	} else {
		pkt.Header.RelayedBy = mySocketAddr
		err := n.conf.Socket.Send(pkt.Header.Destination, pkt, 0)
		if (err!=nil) {
			log.Warn().Msgf("node %s, err:%v",n.addr, err)
		}
	}
}

// Stop implements peer.Service
func (n *node) Stop() error {

	log.Info().Msgf("trying to stop() node %s", n.addr)
	n.stopSigCh<-true
	log.Info().Msgf("trying to stop() node %s, before stopAntiEntropy", n.addr)
	n.stopAntiEntropy()
	log.Info().Msgf("trying to stop() node %s, after stopAntiEntropy", n.addr)
	n.stopHeartbeat()
	log.Info().Msgf("trying to stop() node %s, after stopHeartBeat", n.addr)
	n.pktAckChannels.stopAllWaitingForACK()
	log.Info().Msgf("trying to stop() node %s, after stopAllWaitingForACK", n.addr)
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	//panic("to be implemented in HW0")
	// check if destination is my neighbour, i.e. in my routing table
	n.Lock()
	nextHop,ok := n.routingTable[dest]
	n.Unlock()
	if (ok) {
		// relay should be the address of node who sends the package
		header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)
		pkt := transport.Packet{Header:&header,Msg: &msg}
		err := n.conf.Socket.Send(nextHop, pkt, 0)
		if (err!=nil) {
			log.Info().Msgf("error in unicast %v \n", err)
		}
		return nil
	}
	return errors.New("unicast dest is not neighbour and not in routing table")
}

// relay preserves the original src
func (n *node) Relay(src string, dest string, msg transport.Message) error {
	//panic("to be implemented in HW0")
	// check if destination is my neighbour, i.e. in my routing table
	nextHop,ok := n.routingTable[dest]
	if (ok) {
		// relay should be the address of node who sends the package
		header := transport.NewHeader(src, n.conf.Socket.GetAddress(), dest, 0)
		pkt := transport.Packet{Header:&header,Msg: &msg}
		err := n.conf.Socket.Send(nextHop, pkt, 0)
		if (err!=nil) {
			log.Info().Msgf("error in Relay() %v \n", err)
		}
		return nil
	}
	return errors.New("unicast dest is not neighbour and not in routing table")
}

// create rumor and update my status and rumorsReceived accordingly
func (n *node) createRumor(msg transport.Message) types.Rumor {
	n.Lock()
	defer n.Unlock()
	myCurrSeq := n.Status.getSeqForNode(n.addr)
	rumor := types.Rumor{Origin: n.conf.Socket.GetAddress(), Sequence: myCurrSeq+1, Msg: &msg}
	//n.Status[n.addr] = n.Status[n.addr]+1
	//n.rumorsReceived[n.addr] = append(n.rumorsReceived[n.addr], rumor)
	n.Status.updateForNewRumorFromNode(n.addr, rumor)
	return rumor
}

// when you need to process a message using a handler, you are always provided with both the message and a packet.
// the packet's header is very useful
func (n *node) wrapMsgIntoPacket(msg transport.Message, pkt transport.Packet) transport.Packet{
	//relay := n.GetRoutingTable()[dest]
	//header := transport.NewHeader(n.conf.Socket.GetAddress(), relay, dest, 0)
	newPkt := transport.Packet{
		Header: pkt.Header,
		Msg: &msg,
	}
	return newPkt
}

// AddPeer implements peer.Service
// a peer is a note I can directly relay to
func (n *node) AddPeer(addr ...string) {
	//panic("to be implemented in HW0")
	//myAddr := n.conf.Socket.GetAddress()

	for _,oneAddr := range addr {
		n.SetRoutingEntry(oneAddr, oneAddr)
		n.nbrSet.addNbr(oneAddr)
	}

}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	n.RLock()
	//panic("to be implemented in HW0")
	routingTableCopy := make(map[string]string)
	for k, v := range n.routingTable {
		routingTableCopy[k] = v
	}
	n.RUnlock()
	return routingTableCopy
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	//panic("to be implemented in HW0")
	n.Lock()
	if (relayAddr=="") {
		delete(n.routingTable, origin)
	} else {
		n.routingTable[origin] = relayAddr
		if (origin==relayAddr && origin!=n.addr) {
			// this is a peer, add it to nbrSet
			//n.nbrSet[origin] = true
			n.nbrSet.addNbr(origin)
		}
	}
	n.Unlock()
}

func (n* node) wrapInTransMsgBeforeUnicastOrSend(msg types.Message, msgName string) transport.Message{
	data, err := json.Marshal(&msg)
	if (err!=nil) {
		log.Error().Msgf("err in broadcast():%s", err)
	}
	transportMsg := transport.Message{
		Type:    msgName,
		Payload: data,
	}
	return transportMsg
}
