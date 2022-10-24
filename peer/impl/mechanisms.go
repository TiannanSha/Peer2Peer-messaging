package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/types"
	"time"
)
// **** AntiEntropy mechanism ***
func (n* node) startAntiEntropy() chan struct{} {
	ticker := time.NewTicker(n.conf.AntiEntropyInterval)
	quitCh := make(chan struct{})
	n.antiEntropyQuitCh = quitCh
	go func() {
		for {
			log.Info().Msgf("node %s in startAntiEntropy For Loop", n.addr)
			select {
			case <- ticker.C:
				// send status to a random neighbor
				destNbr,err := n.selectARandomNbrExcept("")
				if (err!=nil) {
					log.Warn().Msgf("node %s err in startAntiEntropy: %s", n.addr, err)
				}
				msg := n.wrapInTransMsgBeforeUnicastOrSend(n.Status, n.Status.Name())
				err = n.Unicast(destNbr, msg)
				if err != nil {
					log.Warn().Msgf("node %s err in startAntiEntropy: %s", n.addr, err)
				}
			case <-quitCh:
				ticker.Stop()
				return
			}
		}
	}()
	return quitCh
}

func (n* node) stopAntiEntropy() {
	log.Info().Msgf("node %s, stopAntiEntropy n.antiEntropyQuitCh %s", n.addr, n.antiEntropyQuitCh)
	if (n.antiEntropyQuitCh != nil) {
		log.Info().Msgf("node %s, stopAntiEntropy(), in if before close n.antiEntropyQuitCh %s", n.addr, n.antiEntropyQuitCh)
		close(n.antiEntropyQuitCh)
		log.Info().Msgf("node %s, stopAntiEntropy(), in if after close n.antiEntropyQuitCh %s", n.addr, n.antiEntropyQuitCh)
	}
}


// *** heartbeat mechanism ***
func (n* node) startHeartbeat() chan struct{} {
	ticker := time.NewTicker(n.conf.HeartbeatInterval)
	quitCh := make(chan struct{})
	n.antiEntropyQuitCh = quitCh
	go func() {
		for {
			select {
			case <- ticker.C:
				// send status to a random neighbor
				emptyMsg := types.EmptyMessage{}
				msg := n.wrapInTransMsgBeforeUnicastOrSend(emptyMsg, emptyMsg.Name())
				err := n.Broadcast(msg)
				if err != nil {
					log.Warn().Msgf("node %s error in startHeartbeat err:", n.addr, err)
				}

			case <-quitCh:
				ticker.Stop()
				return
			}
		}
	}()
	return quitCh
}

func (n* node) stopHeartbeat() {
	if (n.heartbeatQuitCh != nil) {
		close(n.heartbeatQuitCh)
	}
}



