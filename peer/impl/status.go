package impl

import (
	"go.dedis.ch/cs438/types"
	"sync"
)

type status struct {
	StatusMsg types.StatusMessage
	rumorsReceived map[string][]types.Rumor
	sync.Mutex
}

//func (status *status) incrementSeqForNode(node string) {
//	status.Lock()
//	defer status.Unlock()
//	status.StatusMsg[node] = status.StatusMsg[node]+1
//}

func (status *status) getSeqForNode(node string) uint {
	status.Lock()
	defer status.Unlock()
	return status.StatusMsg[node]
}

//func (status *status) setSeqForNode(node string, newSeq uint) {
//	status.Lock()
//	defer status.Unlock()
//	status.StatusMsg[node] = newSeq
//}

func (status *status) getStatusMsg() types.StatusMessage {

	status.Lock()
	defer status.Unlock()
	statusMsgDeepCpy := types.StatusMessage{}
	for k,v := range status.StatusMsg {
		statusMsgDeepCpy[k] = v
	}
	return statusMsgDeepCpy
}

func (status *status) compareWithOthersStatus(statusMsg *types.StatusMessage) (bool,bool, []types.Rumor){
	status.Lock()
	defer status.Unlock()
	IHaveNew := false
	otherHasNew := false
	var rumorsNeedToSend []types.Rumor
	for node,othersSeq := range *statusMsg {
		mySeq := status.StatusMsg[node]
		if (mySeq < othersSeq) {
			otherHasNew = true
		} else if (mySeq > othersSeq) {
			// I have more than this node, so I send all rumors I received but she doesn't have to her in one rumorsMsg
			// concatenate all my additional rumors for each peer
			// seq num starts from one, index starts from 0
			// othersIndex = otherSeq - 1, myIndex = mySeq-1, want [othersIndex+1:myIndex+1]
			IHaveNew = true
			rumorsNeedToSend = append(rumorsNeedToSend, status.rumorsReceived[node][othersSeq:mySeq]...)
		}
	}
	// check for node that exist in my status but not in other's status message
	for node,mySeq := range status.StatusMsg {
		_, inOthers := (*statusMsg)[node]
		if (!inOthers) {
			// for node, I have 1,...,mySeq the other has none, so send index [0, mySeq)
			IHaveNew = true
			rumorsNeedToSend = append(rumorsNeedToSend, status.rumorsReceived[node][:mySeq]...)
		}
	}
	return IHaveNew, otherHasNew, rumorsNeedToSend
}

func (status *status) updateForNewRumorFromNode(node string, rumor types.Rumor) {
	status.Lock()
	defer status.Unlock()
	status.StatusMsg[node] = status.StatusMsg[node]+1
	status.rumorsReceived[node] = append(status.rumorsReceived[node], rumor)
}
