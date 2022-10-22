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
	log.Info().Msgf("for node %v,  Enter ExecRumorsMessage()", n)
	// cast the message to its actual type. You assume it is the right type.
	rumorsMsg, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	log.Info().Msgf("in ExecRumorsMessage, parse out rumorsMsg:%s",rumorsMsg)

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
		nbr,err := n.selectARandomNbrExcept(pkt.Header.Source)
		if (err!=nil) {
			// no suitable neighbour, don't send
			log.Warn().Msgf("error in ExecRumorsMessage():%s", err)
		} else {
			err := n.Relay(pkt.Header.Source, nbr, msg)
			if err != nil {
				log.Warn().Msgf("error in ExecRumorsMessage() after Relay() :%s", err)
			}
		}
	}

	// Send back an AckMessage to the source. need to add src to my routing table
	src := pkt.Header.Source
	n.SetRoutingEntry(src, pkt.Header.RelayedBy)
	ackMsg := types.AckMessage{Status: n.Status, AckedPacketID: pkt.Header.PacketID} //todo refactor wrapping into overloading funcs
	data,err := json.Marshal(ackMsg)
	if (err!=nil) {
		log.Warn().Msgf("err in ExecRumorsMessage: %s", err)
	}
	transportMsg := transport.Message{Type: ackMsg.Name(), Payload: data}
	//transportMsg := n.wrapInTransMsgBeforeUnicastOrSend(ackMsg, ackMsg.Name())
	err = n.Unicast(src, transportMsg)
	if err != nil {
		log.Warn().Msgf("err in ExecRumorsMessage() when calling unicast: %s", err)
	}

	return nil
}

// return whether this rumor message was expected
func (n *node) ExecRumor(rumor types.Rumor, pkt transport.Packet) bool {
	currSeqNum, _ := n.Status[rumor.Origin]
	if (currSeqNum+1) == rumor.Sequence {
		pktInRumor := n.wrapMsgIntoPacket(*rumor.Msg, pkt)
		err := n.conf.MessageRegistry.ProcessPacket(pktInRumor)
		if err != nil {
			log.Warn().Msgf("in ExecRumor(), ProcessPacket returns error: %s", err)
		}
		n.Status[rumor.Origin] = currSeqNum+1
		n.rumorsReceived[rumor.Origin] = append(n.rumorsReceived[rumor.Origin], rumor)
		return true
	} else {
		return false
	}
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

	// do your stuff here with chatMsg...
	// log that message. Nothing else needs to be done. The ChatMessage is parsed
	// by the web-frontend using the message registry.
	log.Info().Msgf("ackMsg:%s",ackMsg)
	return nil
}

// todo should this be locked?
func (n *node) ExecStatusMessage(msg types.Message, pkt transport.Packet) error {
	statusMsg, ok := msg.(*types.StatusMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	IHaveNew := false
	otherHasNew := false
	var rumorsNeedToSend []types.Rumor
	for node,othersSeq := range *statusMsg {
		mySeq := n.Status[node]
		if (mySeq < othersSeq) {
			otherHasNew = true
		} else if (mySeq > othersSeq) {
			// I have more than this node, so I send all rumors I received but she doesn't have to her in one rumorsMsg
			// todo concatenate all my additional rumors for each peer
			// seq num starts from one, index starts from 0
			// othersIndex = otherSeq - 1, myIndex = mySeq-1, want [othersIndex+1:myIndex+1]
			IHaveNew = true
			rumorsNeedToSend = append(rumorsNeedToSend, n.rumorsReceived[node][othersSeq:mySeq]...)
		}
	}

	if otherHasNew {
		// I have less than this node, so I send my status to this node and it will send back more messages
		msg := n.wrapInTransMsgBeforeUnicastOrSend(n.Status, n.Status.Name())
		n.directlySendToNbr(msg, pkt.Header.Source, 0) // todo should ttl be 0 ?
	}
	if IHaveNew {
		// send the accumulated rumors as a rumorsMsg to the peer
		rumorsMessage := types.RumorsMessage{Rumors: rumorsNeedToSend}
		msgToUnicast := n.wrapInTransMsgBeforeUnicastOrSend(rumorsMessage, rumorsMessage.Name())
		n.directlySendToNbr(msgToUnicast, pkt.Header.Source, 0)  // to do should ttl be 0 ?
	}
	if !otherHasNew && !IHaveNew {
		// me and nbr have same status
		// With a certain probability, peer P sends a status message to a random neighbor,
		// different from the one it received the status from.
		if rand.Float64() < n.conf.ContinueMongering {
			newNbr,err := n.selectARandomNbrExcept(pkt.Header.Source)
			if (err!=nil) {
				log.Warn().Msgf("In ExecStatusMessage, err: %s", err)
			}
			statusMsg := n.wrapInTransMsgBeforeUnicastOrSend(n.Status, n.Status.Name())
			n.directlySendToNbr(statusMsg, newNbr, 0)
		}

	}
	return nil
}

/**
 * this function does not use routing table but directly use Send() to send back to nbr
 */
func (n *node) directlySendToNbr(msgToReply transport.Message, nbr string, ttl uint) {
	header := transport.NewHeader(n.addr, nbr, nbr, ttl)
	newPkt := transport.Packet{
		Header: &header,
		Msg:    &msgToReply,
	}
	err := n.conf.Socket.Send(nbr, newPkt, 0)
	if err != nil {
		log.Warn().Msgf("in directlySendToNbr() err: %s", err)
	}
}