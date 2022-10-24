package impl

import "github.com/rs/zerolog/log"

func(n *node) getAckChannel(pktId string) (chan bool, bool){
	n.rwmutexPktAckChannels.Lock()
	ch, ok := n.pktAckChannels[pktId]
	n.rwmutexPktAckChannels.Unlock()
	return ch,ok
}

func (n *node) setAckChannel(pktId string, ch chan bool) {
	n.rwmutexPktAckChannels.Lock()
	n.pktAckChannels[pktId] = ch
	n.rwmutexPktAckChannels.Unlock()
}

func (n *node) deleteAckChannel(pktId string) {
	n.rwmutexPktAckChannels.Lock()
	delete(n.pktAckChannels, pktId)
	n.rwmutexPktAckChannels.Unlock()
}

func(n *node) notifyAckChannel(pktId string) {
	n.rwmutexPktAckChannels.Lock()
	ch, ok := n.pktAckChannels[pktId]
	if !ok {
		log.Error().Msgf("node %s somehow pktId %s doesn't exist!!!!", n.addr, pktId)
		return
	}
	ch <- true
	n.rwmutexPktAckChannels.Unlock()
	return
}

// stop all go routines that are waiting for an ACK
func (n *node) stopAllWaitingForACK() {
	log.Info().Msgf("node %s n.pktAckChannels: %s", n.addr, n.pktAckChannels)
	//n.rwmutexPktAckChannels.Lock()
	for pktId,ch := range n.pktAckChannels {
		log.Info().Msgf("node %s, stopAllWatingForACK(), stop waiting for pktd %s", n.addr, pktId)
		ch <- true
	}
	//n.rwmutexPktAckChannels.Unlock()
	log.Info().Msgf("node %s, end of stopAllWaitingForAck, n.pktAckChannels = %s", n.addr, n.pktAckChannels)
}