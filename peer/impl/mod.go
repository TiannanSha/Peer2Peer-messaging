package impl

import (
	"encoding/json"
	"errors"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"math/rand"
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
	return &node{conf:conf, routingTable: routingTable, stopSigCh: stopSig, sequenceNumber: 0, Status: status}
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
	peers []string
}

// Start implements peer.Service
// peer only can control recv() from socket, doesn't have access to create and close socket, those seem to be the job
// of transport layer (?)
func (n *node) Start() error {
	//panic("to be implemented in HW0")
	// add my addr to the routing table
	// n.AddPeer(n.conf.Socket.GetAddress())
	log.Info().Msg("in node Start()***************")
	//  register handlers for different types of messages
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.ExecChatMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, n.ExecRumorsMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, n.ExecAckMessage)

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
	log.Info().Msg("handlePacket()")
	mySocketAddr := n.conf.Socket.GetAddress()
	if (pkt.Header.Destination == n.conf.Socket.GetAddress() ) {
		err := n.conf.MessageRegistry.ProcessPacket(pkt)
		if (err!=nil) {
			log.Warn().Msgf("in handlePacket(),err:%v",err)
		}
	} else {
		pkt.Header.RelayedBy = mySocketAddr
		err := n.conf.Socket.Send(pkt.Header.Destination, pkt, 0)
		if (err!=nil) {
			log.Warn().Msgf("err:%v",err)
		}
	}
}

// Stop implements peer.Service
func (n *node) Stop() error {
	//panic("to be implemented in HW0")
	log.Print("trying to stop() the peer\n")
	n.stopSigCh<-true
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

func (n *node) createRumor(msg transport.Message) types.Rumor {
	n.sequenceNumber++;
	rumor := types.Rumor{Origin: n.conf.Socket.GetAddress(), Sequence: n.sequenceNumber, Msg: &msg}
	return rumor
}

// Broadcast implements peer.messaging
//- Create a RumorsMessage containing one Rumor (this rumor embeds the message provided in argument), and send it to a random neighbour.
//- Process the message locally
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

	dest := n.peers[rand.Intn(len(n.peers))]
	err = n.Unicast(dest, transportMsg)
	if (err!=nil) {
		log.Warn().Msgf("error in broadcast() after unicast:%s", err)
	}
	//
	//for n1,n2 := range n.routingTable {
	//	// todo check whether this is necessary
	//	// may be should just pick any node and send, or neighbour means a direct neighbour?
	//	if (n1 == n2 && n1!=n.conf.Socket.GetAddress()) {
	//		// this is a direct neighbour
	//		err = n.Unicast(n1, transportMsg)
	//		dest = n1
	//		relay = n2
	//		if (err!=nil) {
	//			log.Warn().Msgf("error in broadcast() after unicast:%s", err)
	//		}
	//		break
	//	}
	//}

	// process messege locally todo what should we do here???
	//header := transport.NewHeader(n.conf.Socket.GetAddress(), relay, dest, 0)
	//pkt := transport.Packet{
	//	Header: &header,
	//	Msg: &transportMsg,
	//}
	//err = n.conf.MessageRegistry.ProcessPacket(pkt)

	return nil
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
		n.peers = append(n.peers, oneAddr)
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
	}
	n.Unlock()
}

/* ********* handlers *********** */

//// The handler. This function will be called when a chat message is received.
//func (n* node) ExecChatMessage(msg types.Message, pkt transport.Packet) error {
//	// cast the message to its actual type. You assume it is the right type.
//	chatMsg, ok := msg.(*types.ChatMessage)
//	if !ok {
//		return xerrors.Errorf("wrong type: %T", msg)
//	}
//
//	// do your stuff here with chatMsg...
//	// log that message. Nothing else needs to be done. The ChatMessage is parsed
//	// by the web-frontend using the message registry.
//	log.Info().Msgf("chatMsg:%s",chatMsg)
//	return nil
//}

// - Send back an AckMessage to the source.
// - Process each Rumor Ʀ by checking if Ʀ is expected or not.
// - Send the RumorMessage to another random neighbor in the case where one of the Rumor data in the packet is expected.
//func (n* node) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
//	log.Info().Msgf("for node %v,  Enter ExecRumorsMessage()", n)
//	// cast the message to its actual type. You assume it is the right type.
//	rumorsMsg, ok := msg.(*types.RumorsMessage)
//	if !ok {
//		return xerrors.Errorf("wrong type: %T", msg)
//	}
//	log.Info().Msgf("in ExecRumorsMessage, parse out rumorsMsg:%s",rumorsMsg)
//
//	/* do your stuff here with rumorsMsg... */
//	// Process each Rumor Ʀ by checking if Ʀ is expected or not
//	atLeastOneRumorExpected := false
//	for _,r:=range rumorsMsg.Rumors {
//		if n.execRumor(r, pkt) {
//			atLeastOneRumorExpected = true
//		}
//	}
//
//	// if one rumor is expected need to send rumorsMsg to another random neighbor
//	if atLeastOneRumorExpected {
//		msg:= n.wrapInTransMsgBeforeUnicast(rumorsMsg, rumorsMsg.Name())
//		nbr,err := n.selectARandomNbrExcept(pkt.Header.Source)
//		if (err!=nil) {
//			// no suitable neighbour, don't send
//			log.Warn().Msgf("error in ExecRumorsMessage():%s", err)
//		} else {
//			n.Unicast(nbr, msg)
//		}
//	}
//
//	// Send back an AckMessage to the source
//	src := pkt.Header.Source
//	statusMsg := n.Status
//	ackMsg := types.AckMessage{Status: statusMsg, AckedPacketID: pkt.Header.PacketID}
//	transportMsg := n.wrapInTransMsgBeforeUnicast(ackMsg, ackMsg.Name())
//	err := n.Unicast(src, transportMsg)
//	if err != nil {
//		log.Warn().Msgf("err in ExecRumorsMessage() when calling unicast: %s", err)
//	}
//
//	return nil
//}

func (n *node) selectARandomNbrExcept(except string) (string, error) {
	for dest,relay := range n.GetRoutingTable() {
		if (dest==relay && dest!=except && dest!=n.conf.Socket.GetAddress()) {
			return dest,nil
		}
	}
	return "", errors.New("no valid neighbor was found")
}

//// return whether this rumor message was expected
//func (n *node)execRumor(rumor types.Rumor, pkt transport.Packet) bool {
//	 currSeqNum, _ := n.Status[rumor.Origin]
//	 if (currSeqNum+1) == rumor.Sequence {
//		 pktInRumor := n.wrapMsgIntoPacket(*rumor.Msg, pkt)
//		 n.conf.MessageRegistry.ProcessPacket(pktInRumor)
//		 return true
//	 } else {
//		 return false
//	 }
//}

// TODO how to write a generic function to wrap diff kinds of msgs to transport msg before unicast
func (n* node) wrapInTransMsgBeforeUnicast(msg types.Message, msgName string) transport.Message{
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
