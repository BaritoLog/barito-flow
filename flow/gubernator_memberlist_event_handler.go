package flow

import "github.com/hashicorp/memberlist"

type gubernatorMemberlistEventHandler struct{}

func (*gubernatorMemberlistEventHandler) NotifyJoin(*memberlist.Node) {}

func (*gubernatorMemberlistEventHandler) NotifyLeave(*memberlist.Node) {}

func (*gubernatorMemberlistEventHandler) NotifyUpdate(*memberlist.Node) {}
