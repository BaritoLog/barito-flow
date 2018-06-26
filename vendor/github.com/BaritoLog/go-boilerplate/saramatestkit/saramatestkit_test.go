package saramatestkit

import (
	"fmt"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/Shopify/sarama"
)

func TestPatchNewClient(t *testing.T) {
	client := NewClient()
	err := fmt.Errorf("some-error")

	patch := PatchNewClient(client, err)
	defer patch.Unpatch()

	client2, err2 := sarama.NewClient(nil, nil)
	FatalIf(t, client2 != client || err2 != err, "sarama.NewClient is not patched")

}
