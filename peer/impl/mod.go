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
	stopSig := make(chan bool)
	status := make(map[string]uint)
	nbrs := make(map[string]bool)
	rumorsReceived := make(map[string][]types.Rumor)
	pktAckChannels := make(map[string]chan bool)

	return &node{conf:conf, routingTable: routingTable, stopSigCh: stopSig, sequenceNumber: 0, Status: status, addr: conf.Socket.GetAddress(), nbrs: nbrs, rumorsReceived: rumorsReceived, pktAckChannels: pktAckChannels}
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
	sequenceNumber uint // sequence number of last created
	Status types.StatusMessage
	nbrs   map[string]bool
	addr   string
	antiEntropyQuitCh chan struct{} // initialized when starting antiEntropy mechanism
	heartbeatQuitCh chan struct{} // initialized when starting heartbeat mechanism
	rumorsReceived map[string][]types.Rumor
	pktAckChannels map[string]chan bool
	rwmutexPktAckChannels sync.RWMutex
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
				pkt, err := n.conf.Socket.Recv(time.Second * 2)
				if errors.Is(err, transport.TimeoutError(0)) {
					log.Info().Msg("in start(), timeout")
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
	n.stopAntiEntropy()
	log.Info().Msgf("trying to stop() node %s, after stopAntiEntropy", n.addr)
	n.stopHeartbeat()
	log.Info().Msgf("trying to stop() node %s, after stopHeartBeat", n.addr)
	//n.stopAllWaitingForACK()
	log.Info().Msgf("trying to stop() node %s, after stopAllWaitingForACK", n.addr)
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	//panic("to be implemented in HW0")
	// check if destination is my neighbour, i.e. in my routing table
	nextHop,ok := n.routingTable[dest]
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
	n.sequenceNumber++;
	rumor := types.Rumor{Origin: n.conf.Socket.GetAddress(), Sequence: n.sequenceNumber, Msg: &msg}
	n.Status[n.addr] = n.Status[n.addr]+1
	n.rumorsReceived[n.addr] = append(n.rumorsReceived[n.addr], rumor)
	return rumor
}

// Broadcast implements peer.messaging
//- Create a RumorsMessage containing one Rumor (this rumor embeds the message provided in argument), and send it to a random neighbour.
//- Process the message locally
// todo since send to immediate nbr, can use send instead of unicast
func (n *node) Broadcast(msg transport.Message) error {
	rumor := n.createRumor(msg)
	rumors := []types.Rumor{rumor}
	rumorsMessage := types.RumorsMessage{Rumors: rumors}
	data, err := json.Marshal(&rumorsMessage)
	if (err!=nil) {
		log.Error().Msgf("err in broadcast():%s", err);
	}
	transportMsg := transport.Message{
		Type:    rumorsMessage.Name(),
		Payload: data,
	}

	var randNbr string
	for randNbr = range(n.nbrs) {
		log.Info().Msgf("**** !!@##@node %s in Broadcast, send to %s, ", n.addr, randNbr)
		break
	}
	pkt := n.directlySendToNbr(transportMsg, randNbr, 0)
	//err = n.Unicast(randNbr, transportMsg)
	//if (err!=nil) {
	//	log.Warn().Msgf("error in broadcast() after unicast:%s", err)
	//}
	//go n.waitForAck(transportMsg, randNbr, pkt.Header.PacketID)

	// process the message locally
	header := transport.NewHeader(n.addr, n.addr, n.addr, 0)
	// let's execute the message (the one embedded in a rumor) "locally"
	pkt = transport.Packet{
		Header: &header,
		Msg: &msg }
	err = n.conf.MessageRegistry.ProcessPacket(pkt)
	if err != nil {
		log.Warn().Msgf("error in Broadcast() when processing msg locally")
	}

	return nil
}

// wait for an ACK from nbr for a broadcast transport message that was sent to nbr
func (n *node) waitForAck(transportMsg transport.Message, prevNbr string, pktIdWaiting string) {
	if (n.conf.AckTimeout==0) { // todo change to ==0
		return
	}
	ch := make(chan bool, 1)
	n.rwmutexPktAckChannels.Lock()
	n.pktAckChannels[pktIdWaiting] = ch
	n.rwmutexPktAckChannels.Unlock()
	select {
		case <- ch:
			log.Info().Msgf("node %s has recevied ack", n.addr)
			// clean up the channel waiting for this packet
			n.rwmutexPktAckChannels.Lock()
			//todo remove pktId in the channels no need to wait for it
			delete(n.pktAckChannels, pktIdWaiting)
			n.rwmutexPktAckChannels.Unlock()
			return
		case <- time.After(n.conf.AckTimeout):
			log.Info().Msgf("node %s has NOT recevied ack, timed out after %s", n.addr, n.conf.AckTimeout)
			// no ack, need to rebroadcast the transport message by sending to a different nbr
			newNbr,err := n.selectARandomNbrExcept(prevNbr)
			if (err!=nil) {
				// todo what to do if there is not another nbr? giveup?
				log.Warn().Msgf("node %s, err in waitForAck", n.addr, err)
				return
			}
			pkt := n.directlySendToNbr(transportMsg, newNbr, 0)
			go n.waitForAck(transportMsg, newNbr, pkt.Header.PacketID)
			// actually when timeout, you just repeat: create a new channel waiting for last sent pktId
			n.rwmutexPktAckChannels.Lock()
			delete(n.pktAckChannels, pktIdWaiting)
			n.rwmutexPktAckChannels.Unlock()
			return
	}
}

// stop all go routines that are waiting for an ACK
func (n *node) stopAllWaitingForACK() {
	log.Info().Msgf("node %s n.pktAckChannels: %s", n.addr, n.pktAckChannels)
	n.rwmutexPktAckChannels.Lock()
	for _,ch := range n.pktAckChannels {
		log.Info().Msgf("node %s, stopAllWatingForACK", n.addr)
		ch <- true
	}
	n.rwmutexPktAckChannels.Unlock()
	log.Info().Msgf("node %s, end of stopAllWaitingForAck", n.addr, n.pktAckChannels)
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
		n.nbrs[oneAddr] = true
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
			// this is a peer, add it to nbrs
			n.nbrs[origin] = true
		}
	}
	n.Unlock()
}

// todo for optimize, maybe can use the nbrs attribute and add lock to this function
func (n *node) selectARandomNbrExcept(except string) (string, error) {
	for dest,relay := range n.GetRoutingTable() {
		if (dest==relay && dest!=except && dest!=n.addr) {
			return dest,nil
		}
	}
	return "", errors.New("no valid neighbor was found")
}

// TODO how to write a generic function to wrap diff kinds of msgs to transport msg before unicast
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
