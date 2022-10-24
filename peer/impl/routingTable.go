package impl

import "sync"

type routingTable struct {
	routingTable map[string]string
	sync.Mutex
}
