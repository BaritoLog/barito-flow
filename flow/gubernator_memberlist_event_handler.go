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
		addresses = append(addresses, address)
	}
	delegate.SetPeersFunc(addresses)
}

func (delegate *gubernatorMemberlistEventHandler) NotifyJoin(node *memberlist.Node) {
	delegate.addressMapMutex.Lock()
	defer delegate.addressMapMutex.Unlock()

	address := fmt.Sprintf("%s:%d", node.Addr.String(), delegate.GRPCPort)
	delegate.prepare()
	delegate.addressMap[address] = struct{}{}

	delegate.renotifyCallback()
}

func (*gubernatorMemberlistEventHandler) NotifyLeave(*memberlist.Node) {}

func (*gubernatorMemberlistEventHandler) NotifyUpdate(*memberlist.Node) {}
