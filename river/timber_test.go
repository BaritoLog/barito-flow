package river

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/BaritoLog/go-boilerplate/strslice"
	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestNewTimberFromRequest(t *testing.T) {

	url := "/str/18/st/1/fw/1/cl/10/produce/kafka-dummy-topic"
	body := strings.NewReader(`{"@message":"hello world", "@timestamp":"2009-11-10T23:00:00Z"}`)
	receivedAt, _ := time.Parse(time.RFC3339, "2009-11-10T23:00:00Z")

	req, err := http.NewRequest("POST", url, body)
	FatalIfError(t, err)

	timber := NewTimberFromRequest(req)
	FatalIf(t, timber.Location != "kafka-dummy-topic", "Wrong timber location: %s", timber.Location)
	FatalIf(t, timber.Message != "hello world", "Wrong timber message: %s", timber.Message)

	trail := timber.ReceiverTrail
	FatalIf(t, trail.URLPath != url, "Wrong trail URL Path: %s", trail.URLPath)
	FatalIf(t, trail.ReceivedAt.Equal(receivedAt), "Wrong trail received at: %v", trail.ReceivedAt)
	FatalIf(t, len(trail.Warnings) > 0, "Trail warning must be zero: %d", len(trail.Warnings))

}

func TestNewTimberFromRequest_NoMessage(t *testing.T) {

	url := "/str/18/st/1/fw/1/cl/10/produce/kafka-dummy-topic"
	body := strings.NewReader(`{"hello":"world", "@timestamp":"2009-11-10T23:00:00Z"}`)

	req, err := http.NewRequest("POST", url, body)
	FatalIfError(t, err)

	timber := NewTimberFromRequest(req)
	FatalIf(t, timber.Message != `{"hello":"world", "@timestamp":"2009-11-10T23:00:00Z"}`,
		"Wrong timber message: %s", timber.Message)

	trail := timber.ReceiverTrail
	FatalIf(t, !strslice.Contain(trail.Warnings, WarnNoMessage),
		"Trails warning must contain '%s': %v", WarnNoMessage, trail.Warnings)
}

func TestNewTimberFromRequest_NoTimestamp(t *testing.T) {
	url := "/str/18/st/1/fw/1/cl/10/produce/kafka-dummy-topic"
	body := strings.NewReader(`{"@message":"hello world"`)

	req, err := http.NewRequest("POST", url, body)
	FatalIfError(t, err)

	timber := NewTimberFromRequest(req)

	trail := timber.ReceiverTrail
	FatalIf(t, trail.ReceivedAt.IsZero(), "Trail received at must be set")
	FatalIf(t, !strslice.Contain(trail.Warnings, WarnNoTimestamp),
		"Trails warning must contain '%s': %v", WarnNoTimestamp, trail.Warnings)

}
