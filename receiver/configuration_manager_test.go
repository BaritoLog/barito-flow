package receiver

import (
	"testing"

	"github.com/imantung/go-boilerplate/testkit"
)

func TestConfigurationManager_Retrieve(t *testing.T) {
	configManager := NewConfigurationManager()

	conf, err := configManager.Retrieve()
	testkit.FatalIfError(t, err)
	testkit.FatalIf(t, conf == nil, "conf is empty")

}
