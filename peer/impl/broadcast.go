package impl

import (
	"encoding/json"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"time"
)

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

	log.Info().Msgf("node broadcast() n.nbrs=%s",n.nbrs)


	log.Info().Msgf("node %s in Broadcast() no neighbour, n.nbrs = %s:" , n.addr, n.nbrs )

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

func (n *node) waitForAck(transportMsg transport.Message, prevNbr string, pktId string) {
	if (n.conf.AckTimeout==0) { // todo change to ==0
		return
	}
	log.Info().Msgf("node %s waitForAck() pktIdWaiting = %s",n.addr, pktId)
	for {
		log.Info().Msgf("node %s waitForAck() pktIdWaiting = %s, inside the for loop",n.addr, pktId)
		ch := make(chan bool, 5)
		n.setAckChannel(pktId, ch)
		select {
		case <- ch:
			log.Info().Msgf("node %s recevied ack for pktId %s", n.addr, pktId)
			close(ch)
			n.deleteAckChannel(pktId)
			return
		case <- time.After(n.conf.AckTimeout):
			log.Info().Msgf("node %s waitForAck() pktIdwaiting %s time out", n.addr, pktId)
			// select a different nbr to send
			newNbr,err := n.selectARandomNbrExcept(prevNbr)
			if (err != nil) {
				log.Info().Msgf("node %s waitFor ack err: %s" ,err)
			}
			n.directlySendToNbr(transportMsg, newNbr, 0)
			continue
		}
	}
}
