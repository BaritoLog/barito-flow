package instru

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type webCallback struct {
	url string
}

func NewWebCallback(url string) Callback {
	return &webCallback{url}
}

func (c *webCallback) OnCallback(instr Instrumentation) (err error) {
	b, _ := json.Marshal(instr)

	resp, err := http.Post(c.url, "application/json", bytes.NewReader(b))
	if err != nil {
		return
	}

	if resp.StatusCode != 200 {
		err = fmt.Errorf("Got status code %d", resp.StatusCode)
	}

	return
}
