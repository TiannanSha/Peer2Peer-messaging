package impl

import (
	"encoding/json"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"math/rand"
)

func (n* node) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
	log.Info().Msgf("for node %s,  Enter ExecRumorsMessage()", n.addr)
	// cast the message to its actual type. You assume it is the right type.
	rumorsMsg, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	log.Info().Msgf("node %s, in ExecRumorsMessage, parse out rumorsMsg:%s",n.addr , rumorsMsg)

	/* do your stuff here with rumorsMsg... */
	// Process each Rumor Ʀ by checking if Ʀ is expected or not
	atLeastOneRumorExpected := false
	for _,r:=range rumorsMsg.Rumors {
		if n.ExecRumor(r, pkt) {
			atLeastOneRumorExpected = true
		}
	}

	// if one rumor is expected need to send rumorsMsg to another random neighbor
	// when you spread a rumor the source should still be the original src
	if atLeastOneRumorExpected {
		msg:= n.wrapInTransMsgBeforeUnicastOrSend(rumorsMsg, rumorsMsg.Name())
		nbr,err := n.nbrSet.selectARandomNbrExcept(pkt.Header.Source)
		if (err!=nil) {
			// no suitable neighbour, don't send
			log.Warn().Msgf("node %s error in ExecRumorsMessage():%s", n.addr, err)
		} else {
			err := n.Relay(pkt.Header.Source, nbr, msg)
			if err != nil {
				log.Warn().Msgf("node %s , error in ExecRumorsMessage() after Relay() :%s", n.addr, err)
			}
		}
	}

	// Send back an AckMessage to the relayby. need to add src to my routing table
	// if this is my own message, no need to reply ack
	src := pkt.Header.Source
	if (src==n.addr) {
		return nil
	}
	//n.SetRoutingEntry(src, pkt.Header.RelayedBy)
	ackMsg := types.AckMessage{Status: n.Status.getStatusMsg(), AckedPacketID: pkt.Header.PacketID}
	data,err := json.Marshal(ackMsg)
	if (err!=nil) {
		log.Warn().Msgf("node %s, err in ExecRumorsMessage: %s", n.addr, err)
	}
	transportMsg := transport.Message{Type: ackMsg.Name(), Payload: data}
	//transportMsg := n.wrapInTransMsgBeforeUnicastOrSend(ackMsg, ackMsg.Name())
	n.directlySendToNbr(transportMsg, pkt.Header.RelayedBy)

	// used to be unicast ack to src
	//err = n.Unicast(src, transportMsg)
	//if err != nil {
	//	log.Warn().Msgf("node %s , err in ExecRumorsMessage() when calling unicast: %s", n.addr, err)
	//}

	return nil
}

// return whether this rumor message was expected
func (n *node) ExecRumor(rumor types.Rumor, pkt transport.Packet) bool {
	//currSeqNum, _ := n.StatusMsg[rumor.Origin]
	currSeqNum := n.Status.getSeqForNode(rumor.Origin)
	if (currSeqNum+1) == rumor.Sequence {
		pktInRumor := n.wrapMsgIntoPacket(*rumor.Msg, pkt)
		err := n.conf.MessageRegistry.ProcessPacket(pktInRumor)
		if err != nil {
			log.Warn().Msgf("node %s in ExecRumor(), ProcessPacket returns error: %s", n.addr, err)
		}
		//n.StatusMsg[rumor.Origin] = currSeqNum+1
		//n.Status.incrementSeqForNode(rumor.Origin)
		//n.rumorsReceived[rumor.Origin] = append(n.rumorsReceived[rumor.Origin], rumor)
		n.Status.updateForNewRumorFromNode(rumor.Origin, rumor)
		originIsNbr := n.nbrSet.contains(rumor.Origin)
		if !originIsNbr {
			n.SetRoutingEntry(rumor.Origin, pkt.Header.RelayedBy)
		}
		return true
	}
	return false
}

// The handler. This function will be called when a chat message is received.
func (n* node) ExecChatMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	chatMsg, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// do your stuff here with chatMsg...
	// log that message. Nothing else needs to be done. The ChatMessage is parsed
	// by the web-frontend using the message registry.
	log.Info().Msgf("chatMsg:%s",chatMsg)
	return nil
}

func (n* node) ExecAckMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	ackMsg, ok := msg.(*types.AckMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// notify the go routine waiting for pktId that we received ack for it
	n.pktAckChannels.notifyAckChannel(ackMsg.AckedPacketID)

	// process the status message inside the ack message
	statusMsg := n.wrapInTransMsgBeforeUnicastOrSend(ackMsg.Status, ackMsg.Status.Name())
	newPkt := n.wrapMsgIntoPacket(statusMsg, pkt)
	err := n.conf.MessageRegistry.ProcessPacket(newPkt)
	if err != nil {
		log.Info().Msgf("node %s err in ExecAckMessage(): %s", n.addr, err)
	}

	return nil
}

func (n *node) ExecPrivateMessage(msg types.Message, pkt transport.Packet) error {
	privateMsg, ok := msg.(*types.PrivateMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	_,IamRecipient := privateMsg.Recipients[n.addr]
	if IamRecipient {
		newPkt :=n.wrapMsgIntoPacket(*privateMsg.Msg, pkt)
		err := n.conf.MessageRegistry.ProcessPacket(newPkt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *node) ExecStatusMessage(msg types.Message, pkt transport.Packet) error {
	log.Info().Msgf("**** !@##@node %s enters ExecStatusMessage()", n.addr)
	statusMsg, ok := msg.(*types.StatusMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	IHaveNew, otherHasNew, rumorsNeedToSend := n.Status.compareWithOthersStatus(statusMsg)

	//log.Info().Msgf("**** !@##@node %s enters ExecStatusMessage(), " +
	//	"otherHasNew=%s, IHaveNew=", n.addr, otherHasNew, IHaveNew)
	if otherHasNew {
		// I have less than this node, so I send my status to this node and it will send back more messages
		msg := n.wrapInTransMsgBeforeUnicastOrSend(n.Status.getStatusMsg(), n.Status.getStatusMsg().Name())
		n.directlySendToNbr(msg, pkt.Header.Source)
	}
	if IHaveNew {
		// send the accumulated rumors as a rumorsMsg to the peer
		rumorsMessage := types.RumorsMessage{Rumors: rumorsNeedToSend}
		msgToUnicast := n.wrapInTransMsgBeforeUnicastOrSend(rumorsMessage, rumorsMessage.Name())
		n.directlySendToNbr(msgToUnicast, pkt.Header.Source)  // to do should ttl be 0 ?
	}
	if !otherHasNew && !IHaveNew {
		// me and nbr have same status
		// With a certain probability, peer P sends a status message to a random neighbor,
		// different from the one it received the status from.
		if rand.Float64() < n.conf.ContinueMongering {
			newNbr,err := n.nbrSet.selectARandomNbrExcept(pkt.Header.Source)
			if (err!=nil) {
				log.Warn().Msgf("Node %s,In ExecStatusMessage, err: %s", n.addr, err)
			}
			if newNbr!="" {
				// successfully get a random nbr
				statusMsg := n.wrapInTransMsgBeforeUnicastOrSend(n.Status.getStatusMsg(),
					n.Status.getStatusMsg().Name())
				n.directlySendToNbr(statusMsg, newNbr)
			}
		}

	}
	return nil
}

/**
 * this function does not use routing table but directly use Send() to send back to nbr
 * @return packet it sent
 */
func (n *node) directlySendToNbr(msgToReply transport.Message, nbr string) transport.Packet {
	header := transport.NewHeader(n.addr, n.addr, nbr, 0)
	newPkt := transport.Packet{
		Header: &header,
		Msg:    &msgToReply,
	}
	err := n.conf.Socket.Send(nbr, newPkt, 0)
	if err != nil {
		log.Warn().Msgf("node %s, send to nbr %s, in directlySendToNbr() err: %s", n.addr,nbr, err)
		log.Warn().Msgf("in directlySendToNbr() err: %s", err)
	}
	return newPkt
}