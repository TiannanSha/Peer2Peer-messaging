package impl

import (
	"github.com/rs/zerolog/log"
	"sync"
)

type chanPool struct {
	pktAckChannels map[string]chan bool
	sync.Mutex
}

func(c *chanPool) getAckChannel(pktId string) (chan bool, bool){
	c.Lock()
	defer c.Unlock()
	ch, ok := c.pktAckChannels[pktId]

	return ch,ok
}

func (c *chanPool) setAckChannel(pktId string, ch chan bool) {
	c.Lock()
	defer c.Unlock()
	c.pktAckChannels[pktId] = ch
}

func (c *chanPool) deleteAckChannel(pktId string) {
	c.Lock()
	defer c.Unlock()
	delete(c.pktAckChannels, pktId)
}

func(c *chanPool) notifyAckChannel(pktId string) {
	ch, ok := c.getAckChannel(pktId)
	if !ok {
		return
	}
	ch <- true
	return
}

// stop all go routines that are waiting for an ACK
func (c *chanPool) stopAllWaitingForACK() {
	log.Info().Msgf("in  stopAllWaitingForACK()")
	c.Lock()
	//log.Info().Msgf("node %s n.pktAckChannels: %s", n.addr, n.pktAckChannels)
	for _,ch := range c.pktAckChannels {
		ch <- true
	}
	//log.Info().Msgf("node %s, end of stopAllWaitingForAck, n.pktAckChannels = %s", n.addr, n.pktAckChannels)
	c.Unlock()
}