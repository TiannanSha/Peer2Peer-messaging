package impl

import (
	"github.com/rs/zerolog/log"
	"time"
)

func (n* node) startAntiEntropy() chan struct{} {
	ticker := time.NewTicker(n.conf.AntiEntropyInterval)
	quitCh := make(chan struct{})
	n.antiEntropyQuitCh = quitCh
	go func() {
		for {
			select {
			case <- ticker.C:
				// send status to a random neighbor
				destNbr,err := n.selectARandomNbrExcept("")
				if (err!=nil) {
					log.Warn().Msgf("err in startAntiEntropy: %s",err)
				}
				msg := n.wrapInTransMsgBeforeUnicastOrSend(n.Status, n.Status.Name())
				err = n.Unicast(destNbr, msg)
				if err != nil {
					log.Warn().Msgf("err in startAntiEntropy: %s",err)
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
	if (n.antiEntropyQuitCh != nil) {
		close(n.antiEntropyQuitCh)
	}
}



