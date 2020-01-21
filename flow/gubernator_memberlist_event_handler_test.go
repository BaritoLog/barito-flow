package flow

import "testing"

import "github.com/hashicorp/memberlist"

func TestGubernatorMemberlistEventHandler_InterfaceCompliance(t *testing.T) {
	var _ memberlist.EventDelegate = &gubernatorMemberlistEventHandler{}
}
