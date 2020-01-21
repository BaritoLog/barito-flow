package flow

import (
	"net"
	"testing"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/assert"
)

func TestGubernatorMemberlistEventHandler_InterfaceCompliance(t *testing.T) {
	var _ memberlist.EventDelegate = &gubernatorMemberlistEventHandler{}
}

func TestGubernatorMemberlistEventHandler_NotifyJoin(t *testing.T) {
	var returnedAddresses []string
	delegate := &gubernatorMemberlistEventHandler{
		GRPCPort: 2022,
		SetPeersFunc: func(addresses []string) {
			returnedAddresses = addresses
		},
	}

	t.Run("first", func(t *testing.T) {
		delegate.NotifyJoin(&memberlist.Node{
			Name: "node-01",
			Addr: net.IPv4(192, 168, 0, 1),
		})

		assert.ElementsMatch(t, []string{"192.168.0.1:2022"}, returnedAddresses)
	})

	t.Run("other", func(t *testing.T) {
		delegate.NotifyJoin(&memberlist.Node{
			Name: "node-02",
			Addr: net.IPv4(192, 168, 0, 2),
		})

		assert.ElementsMatch(t, []string{
			"192.168.0.1:2022",
			"192.168.0.2:2022",
		}, returnedAddresses)
	})
}

func TestGubernatorMemberlistEventHandler_NotifyLeave(t *testing.T) {
	var returnedAddresses []string
	delegate := &gubernatorMemberlistEventHandler{
		GRPCPort: 2022,
		SetPeersFunc: func(addresses []string) {
			returnedAddresses = addresses
		},
	}

	delegate.NotifyJoin(&memberlist.Node{
		Name: "node-01",
		Addr: net.IPv4(192, 168, 0, 1),
	})
	delegate.NotifyJoin(&memberlist.Node{
		Name: "node-02",
		Addr: net.IPv4(192, 168, 0, 2),
	})

	t.Run("first", func(t *testing.T) {
		delegate.NotifyLeave(&memberlist.Node{
			Addr: net.IPv4(192, 168, 0, 1),
		})

		assert.ElementsMatch(t, []string{"192.168.0.2:2022"}, returnedAddresses)
	})
}
