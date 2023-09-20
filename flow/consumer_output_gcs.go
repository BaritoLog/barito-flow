package flow

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/BaritoLog/barito-flow/flow/types"
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
)

var ErrorGCSBufferFull = fmt.Errorf("GCS buffer is full")
var ErrorGCSStop = fmt.Errorf("GCS is stopped")

var _ types.ConsumerOutput = &GCS{}

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type GCSSettings struct {
	ProjectID          string `envconfig:"project_id" required:"true"`
	BucketName         string `envconfig:"bucket_name" required:"true" `
	BucketPath         string `envconfig:"bucket_path" required:"true"`
	ServiceAccountPath string `envconfig:"service_account_path" required:"true"`

	// TODO: increase this after tests
	FlushMaxBytes       int `envconfig:"flush_max_bytes" default:"52428800"` // 50MB
	FlushMaxTimeSeconds int `envconfig:"flush_max_time" default:"3600"`      // 1 hour
}

// TODO: metrics instrumentation
type GCS struct {
	name          string
	projectID     string
	bucketName    string
	bucketPath    string
	storageClient *storage.Client

	flushMaxBytes int
	flushMaxTime  time.Duration

	// TODO: use file
	buffer      *bytes.Buffer
	onFlushFunc []func() error

	uploadFunc func() error

	logger *log.Logger
	mu     sync.Mutex
	isStop bool
	clock  Clock
}

func NewGCSFromEnv(name string) *GCS {
	var settings GCSSettings
	err := envconfig.Process("GCS", &settings)
	if err != nil {
		err := fmt.Errorf("GCS settings are not properly set: %s", err.Error())
		panic(err)
	}

	storageClient, err := storage.NewClient(
		context.Background(),
		option.WithCredentialsFile(settings.ServiceAccountPath),
	)

	if err != nil {
		panic(err)
	}

	g := &GCS{
		name:          name,
		projectID:     settings.ProjectID,
		bucketName:    settings.BucketName,
		bucketPath:    settings.BucketPath,
		flushMaxBytes: settings.FlushMaxBytes,
		flushMaxTime:  time.Duration(settings.FlushMaxTimeSeconds) * time.Second,
		logger:        log.New().WithField("component", "GCS").WithField("name", name).Logger,
		clock:         &realClock{},
		storageClient: storageClient,

		buffer:      bytes.NewBuffer([]byte{}),
		onFlushFunc: make([]func() error, 0),
	}
	g.uploadFunc = g.uploadToGCS

	return g
}

// callback to the producer when we flush
func (g *GCS) AddOnFlushFunc(f func() error) {
	g.onFlushFunc = append(g.onFlushFunc, f)
}

// it will reject when buffer is full, the client should retry indefinitely
func (g *GCS) OnMessage(msg []byte) error {
	if g.isStop {
		return ErrorGCSStop
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.buffer.Len() >= g.flushMaxBytes {
		return ErrorGCSBufferFull
	}

	_, err := g.buffer.Write(msg)
	g.buffer.WriteString("\n")
	return err
}

// stopping the output, and flush the buffer
func (g *GCS) Stop() {
	g.logger.Info("Stopping GCS, flushing")
	g.isStop = true
	g.Flush()
}

// it will create forever loop to check if flush is needed
func (g *GCS) Start() error {
	ticker := time.NewTicker(g.flushMaxTime)
	var numBytes int

	for {
		if g.isStop {
			return nil
		}

		g.mu.Lock()
		numBytes = g.buffer.Len()
		g.logger.Warn("buffer size: ", numBytes)
		g.mu.Unlock()

		if g.flushMaxBytes > 0 && numBytes >= g.flushMaxBytes {
			g.logger.Info("buffer is full, flushing")
			g.Flush()
			ticker.Reset(g.flushMaxTime)
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

// TODO: use compression
func (g *GCS) uploadToGCS() error {
	ctx := context.TODO()

	filename := fmt.Sprintf("%s/%s/%s-%s.log", g.bucketPath, g.name, g.name, g.clock.Now().Format(time.RFC3339))
	bucket := g.storageClient.Bucket(g.bucketName)
	obj := bucket.Object(filename)
	w := obj.NewWriter(ctx)
	defer w.Close()

	if _, err := io.Copy(w, g.buffer); err != nil {
		g.logger.Error(err)
		return err
	}
	return w.Close()
}

// will call the uploadFunc and onFlushFunc, and then reset the buffer
func (g *GCS) Flush() {
	g.logger.Info("Flushing GCS")
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.buffer.Len() == 0 {
		g.logger.Info("buffer is empty, skip flushing")
		return
	}

	// TODO: retry indefinitely
	err := g.uploadFunc()
	if err != nil {
		g.logger.Error(err)
		return
	}

	for _, f := range g.onFlushFunc {
		err := f()
		if err != nil {
			g.logger.Error(err.Error())
		}
	}

	g.buffer.Reset()
}
