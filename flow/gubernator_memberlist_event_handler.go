package flow

import "github.com/hashicorp/memberlist"

type gubernatorMemberlistEventHandler struct {
	GRPCPort     uint16
	SetPeersFunc func(addresses []string)
}

func (delegate *gubernatorMemberlistEventHandler) NotifyJoin(*memberlist.Node) {
	delegate.SetPeersFunc([]string{"192.168.0.1:2022"})
}

func (*gubernatorMemberlistEventHandler) NotifyLeave(*memberlist.Node) {}

func (*gubernatorMemberlistEventHandler) NotifyUpdate(*memberlist.Node) {}
