package impl

import (
	"errors"
	"math/rand"
	"sync"
)

type nbrSet struct {
	nbrs map[string]bool
	sync.Mutex
}

func (nbrSet *nbrSet) selectARandomNbrExcept(except string) (string, error) {
	nbrSet.Lock()
	defer nbrSet.Unlock()
	var candidates []string
	for nbr := range nbrSet.nbrs {
		if (nbr != except) {
			candidates = append(candidates,nbr)
		}
	}
	if len(candidates)==0 {
		return "", errors.New("no valid neighbor was found")
	}
	return candidates[rand.Intn(len(candidates))], nil
}

func (nbrSet *nbrSet) addNbr(newNbr string) {
	nbrSet.Lock()
	defer nbrSet.Unlock()
	nbrSet.nbrs[newNbr] = true
}

func (nbrSet *nbrSet) contains(node string) bool {
	nbrSet.Lock()
	defer nbrSet.Unlock()
	_,ok := nbrSet.nbrs[node]
	return ok
}


