package flow

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/BaritoLog/barito-flow/flow/types"
	"github.com/BaritoLog/barito-flow/prome"
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
)

var _ types.ConsumerOutput = &VLogs{}

var ErrorVLogsStop = fmt.Errorf("VLogs is stopped")
var ErrorVLogsBufferFull = fmt.Errorf("VLogs buffer is full")

type VLogsSettings struct {
	Endpoint            string `envconfig:"endpoint" required:"true"`
	FlushMaxBatches     int    `envconfig:"flush_max_batches" default:"5000"`    // max number of batches to flush at once
	FlushMaxTimeSeconds int    `envconfig:"flush_max_time_seconds" default:"10"` // max time to wait before flushing in seconds
}

type VLogs struct {
	name     string
	endpoint string

	httpClient *http.Client

	flushMaxBatches int
	flushMaxTime    time.Duration

	buffer      *bytes.Buffer
	onFlushFunc []func() error
	uploadFunc  func() error

	logger     *log.Entry
	mu         sync.Mutex
	isStop     bool
	logCounter int
}

func NewVLogsFromEnv(name string) *VLogs {
	var settings VLogsSettings
	err := envconfig.Process("VLOGS", &settings)
	if err != nil {
		err := fmt.Errorf("VLogs settings are not properly set: %s", err.Error())
		panic(err)
	}

	logger := log.New().WithField("component", "VLogs").WithField("name", name)

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	logger.Info("Created VLogs client")

	name = strings.TrimSuffix(name, "_pb")
	g := &VLogs{
		name:            name,
		endpoint:        settings.Endpoint,
		httpClient:      httpClient,
		flushMaxBatches: settings.FlushMaxBatches,
		flushMaxTime:    time.Duration(settings.FlushMaxTimeSeconds) * time.Second,
		logger:          logger,

		buffer:      bytes.NewBuffer(nil),
		onFlushFunc: make([]func() error, 0),
	}
	g.uploadFunc = g.pushToVLogs

	return g
}

// callback to the producer when we flush
func (g *VLogs) AddOnFlushFunc(f func() error) {
	g.onFlushFunc = append(g.onFlushFunc, f)
}

// it will reject when buffer is full, the client should retry indefinitely
func (g *VLogs) OnMessage(msg []byte) error {
	if g.isStop {
		return ErrorVLogsStop
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.logCounter >= g.flushMaxBatches {
		prome.IncreaseConsumerCustomErrorTotalf("vlogs_buffer_full: %s", g.name)
		return ErrorVLogsBufferFull
	}

	_, err := g.buffer.Write(msg)
	g.logCounter++
	g.buffer.Write([]byte("\n"))
	return err
}

// stopping the output, and flush the buffer
func (g *VLogs) Stop() {
	g.logger.Info("Stopping VLogs, flushing")
	g.isStop = true
	g.Flush()
}

// it will create forever loop to check if flush is needed
func (g *VLogs) Start() error {
	ticker := time.NewTicker(g.flushMaxTime)

	for {
		if g.isStop {
			return nil
		}

		g.mu.Lock()
		numLog := g.logCounter
		g.mu.Unlock()

		if g.flushMaxBatches > 0 && numLog >= g.flushMaxBatches {
			g.logger.Info("buffer is full, flushing")
			g.Flush()
			ticker.Reset(g.flushMaxTime)
			time.Sleep(1 * time.Second)
			continue
		}

		// check if flush is needed depend of max item, max bytes, and max time
		select {
		case <-ticker.C:
			g.logger.Info("max time reached, flushing")
			g.Flush()
			ticker.Reset(g.flushMaxTime)
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

func (g *VLogs) pushToVLogs() error {
	g.logger.Debug("pushToVLogs", g.name)

	req, err := http.NewRequest("POST", g.endpoint, g.buffer)
	if err != nil {
		prome.IncreaseConsumerCustomErrorTotalf("vlogs_request_creation_failed: %s", g.name)
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("VL-Msg-Field", "@message")
	req.Header.Set("VL-Extra-Fields", fmt.Sprintf("app_group=%s,app_name=%s", "hoke", g.name))
	req.Header.Set("VL-Stream-Fields", "app_group,app_name")
	req.Header.Set("VL-Time-Field", "client_trail.sent_at")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		prome.IncreaseConsumerCustomErrorTotalf("vlogs_request_failed: %s", g.name)
		return fmt.Errorf("failed to send request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		prome.IncreaseConsumerCustomErrorTotalf("vlogs_request_failed: %s", g.name)
		body, _ := io.ReadAll(resp.Body)
		fmt.Println("Response body:", string(body))
		return fmt.Errorf("failed to send request, status code: %d", resp.StatusCode)
	}

	// TODO: metrics

	return nil
}

// will call the uploadFunc and onFlushFunc, and then reset the buffer
func (g *VLogs) Flush() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.logCounter == 0 {
		g.logger.Info("buffer is empty, skip flushing")
		return
	}
	g.logger.Debug("Flushing VLogs", g.name, " with logCounter:", g.logCounter)

	// TODO: retry indefinitely
	err := g.uploadFunc()
	if err != nil {
		g.logger.Error(err)
		return
	}
	g.logger.Warn("flushed to VLogs")

	g.logger.Warn("calling onFlushFunc...")
	for _, f := range g.onFlushFunc {
		err := f()
		if err != nil {
			prome.IncreaseConsumerCustomErrorTotalf("vlogs_on_flush_func_failed: %s", g.name)
			g.logger.Error(err.Error())
		}
	}
	g.logger.Warn("onFlushFuncs called")

	// recreate the buffer
	g.buffer = bytes.NewBuffer(nil)
	g.logCounter = 0
}
