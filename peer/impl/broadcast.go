package impl

import (
	"encoding/json"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"time"
)

// Broadcast implements peer.messaging
//- Create a RumorsMessage containing one Rumor (this rumor embeds the message provided in argument),
//  and send it to a random neighbour.
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

	//var randNbr string
	//for randNbr = range(n.nbrSet) {
	//	log.Info().Msgf("**** !!@##@node %s in Broadcast, send to %s, ", n.addr, randNbr)
	//	break
	//}
	randNbr,err := n.nbrSet.selectARandomNbrExcept("")
	if (err!=nil) {
		log.Info().Msgf("node %s, in broadcast after selectARandomNbrExcept, err: %s", n.addr, err)
	}
	pkt := n.directlySendToNbr(transportMsg, randNbr)
	//err = n.Unicast(randNbr, transportMsg)
	//if (err!=nil) {
	//	log.Warn().Msgf("error in broadcast() after unicast:%s", err)
	//}
	go n.waitForAck(transportMsg, randNbr, pkt.Header.PacketID)

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

func (n *node) waitForAck(transportMsg transport.Message, prevNbr string, pktID string) {
	if (n.conf.AckTimeout==0) {
		return
	}
	log.Info().Msgf("node %s waitForAck() pktIdWaiting = %s",n.addr, pktID)
	for {
		log.Info().Msgf("node %s waitForAck() pktIdWaiting = %s, inside the for loop",n.addr, pktID)
		ch := make(chan bool, 5)
		n.pktAckChannels.setAckChannel(pktID, ch)
		select {
		case <- ch:
			log.Info().Msgf("node %s recevied ack for pktId %s", n.addr, pktID)
			close(ch)
			n.pktAckChannels.deleteAckChannel(pktID)
			return
		case <- time.After(n.conf.AckTimeout):
			log.Info().Msgf("node %s waitForAck() pktIdwaiting %s time out", n.addr, pktID)
			// select a different nbr to send
			newNbr,err := n.nbrSet.selectARandomNbrExcept(prevNbr)
			if (err != nil) {
				log.Info().Msgf("node %s waitFor ack err: %s" ,n.addr, err)
			}
			n.directlySendToNbr(transportMsg, newNbr)
			continue
		}
	}
}


