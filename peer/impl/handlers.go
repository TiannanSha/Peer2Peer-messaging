package impl

import (
	"encoding/json"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
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
		if n.execRumor(r, pkt) {
			atLeastOneRumorExpected = true
		}
	}

	// if one rumor is expected need to send rumorsMsg to another random neighbor
	if atLeastOneRumorExpected {
		msg:= n.wrapInTransMsgBeforeUnicast(rumorsMsg, rumorsMsg.Name())
		nbr,err := n.selectARandomNbrExcept(pkt.Header.Source)
		if (err!=nil) {
			// no suitable neighbour, don't send
			log.Warn().Msgf("error in ExecRumorsMessage():%s", err)
		} else {
			n.Unicast(nbr, msg)
		}
	}

	// Send back an AckMessage to the source. need to add src to be my peer first
	src := pkt.Header.Source
	n.AddPeer(src)
	ackMsg := types.AckMessage{Status: n.Status, AckedPacketID: pkt.Header.PacketID} //todo refactor wrapping into overloading funcs
	data,err := json.Marshal(ackMsg)
	if (err!=nil) {
		log.Warn().Msgf("err in ExecRumorsMessage: %s", err)
	}
	transportMsg := transport.Message{Type: ackMsg.Name(), Payload: data}
	//transportMsg := n.wrapInTransMsgBeforeUnicast(ackMsg, ackMsg.Name())
	err = n.Unicast(src, transportMsg)
	if err != nil {
		log.Warn().Msgf("err in ExecRumorsMessage() when calling unicast: %s", err)
	}

	return nil
}

// return whether this rumor message was expected
func (n *node)execRumor(rumor types.Rumor, pkt transport.Packet) bool {
	currSeqNum, _ := n.Status[rumor.Origin]
	if (currSeqNum+1) == rumor.Sequence {
		pktInRumor := n.wrapMsgIntoPacket(*rumor.Msg, pkt)
		n.conf.MessageRegistry.ProcessPacket(pktInRumor)
		n.Status[rumor.Origin] = currSeqNum+1
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