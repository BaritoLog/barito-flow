package flow

import (
	"fmt"
	"sync"

	"github.com/hashicorp/memberlist"
)

type gubernatorMemberlistEventHandler struct {
	GRPCPort     uint16
	SetPeersFunc func(addresses []string)

	addressMapMutex sync.Mutex
	addressMap      map[string]struct{}
}

func (delegate *gubernatorMemberlistEventHandler) prepare() {
	if delegate.addressMap == nil {
		delegate.addressMap = make(map[string]struct{})
	}
}

func (delegate *gubernatorMemberlistEventHandler) renotifyCallback() {
	var addresses []string
	for address := range delegate.addressMap {
		addresses = append(addresses, fmt.Sprintf("%s:%d", address, delegate.GRPCPort))
	}
	delegate.SetPeersFunc(addresses)
}

func (delegate *gubernatorMemberlistEventHandler) NotifyJoin(node *memberlist.Node) {
	delegate.addressMapMutex.Lock()
	defer delegate.addressMapMutex.Unlock()

	delegate.prepare()
	delegate.addressMap[node.Addr.String()] = struct{}{}
	delegate.renotifyCallback()
}

func (delegate *gubernatorMemberlistEventHandler) NotifyLeave(node *memberlist.Node) {
	delegate.addressMapMutex.Lock()
	defer delegate.addressMapMutex.Unlock()

	delegate.prepare()
	delete(delegate.addressMap, node.Addr.String())
	delegate.renotifyCallback()
}

func (*gubernatorMemberlistEventHandler) NotifyUpdate(*memberlist.Node) {}
