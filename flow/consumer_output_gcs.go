package flow

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/storage"

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
	ProjectID  string `envconfig:"project_id" required:"true"`
	BucketName string `envconfig:"bucket_name" required:"true" `
	BucketPath string `envconfig:"bucket_path" required:"true"`

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
	buffer      io.ReadWriteCloser
	onFlushFunc []func() error

	uploadFunc func() error

	logger       *log.Logger
	mu           sync.Mutex
	isStop       bool
	clock        Clock
	bytesCounter int
}

func NewGCSFromEnv(name string) *GCS {
	var settings GCSSettings
	err := envconfig.Process("GCS", &settings)
	if err != nil {
		err := fmt.Errorf("GCS settings are not properly set: %s", err.Error())
		panic(err)
	}

	logger := log.New().WithField("component", "GCS").WithField("name", name).Logger

	storageClient, err := storage.NewClient(
		context.Background(),
	)

	if err != nil {
		// TODO: should not panic
		panic(err)
	}

	logger.Info("Created GCS client")

	buffer, err := NewFileBuffer(name)
	if err != nil {
		// TODO: should not panic
		panic(err)
	}

	g := &GCS{
		name:          name,
		projectID:     settings.ProjectID,
		bucketName:    settings.BucketName,
		bucketPath:    settings.BucketPath,
		flushMaxBytes: settings.FlushMaxBytes,
		flushMaxTime:  time.Duration(settings.FlushMaxTimeSeconds) * time.Second,
		logger:        logger,
		clock:         &realClock{},
		storageClient: storageClient,

		buffer:      buffer,
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

	if g.bytesCounter >= g.flushMaxBytes {
		return ErrorGCSBufferFull
	}

	n, err := g.buffer.Write(msg)
	g.bytesCounter += n
	g.buffer.Write([]byte("\n"))
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
		numBytes = g.bytesCounter
		g.logger.Warn("buffer size: ", numBytes)
		g.mu.Unlock()

		if g.flushMaxBytes > 0 && numBytes >= g.flushMaxBytes {
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

// TODO: use compression
func (g *GCS) uploadToGCS() error {
	ctx := context.TODO()

	filename := fmt.Sprintf("%s/%s/%s-%s.log", g.bucketPath, g.name, g.name, g.clock.Now().Format(time.RFC3339))
	bucket := g.storageClient.Bucket(g.bucketName)
	obj := bucket.Object(filename)
	w := obj.NewWriter(ctx)
	defer w.Close()

	n, err := io.Copy(w, g.buffer)
	if err != nil {
		g.logger.Error(err)
		return err
	}

	g.logger.Infof("Uploaded %s, %d bytes", filename, n)
	return w.Close()
}

// will call the uploadFunc and onFlushFunc, and then reset the buffer
func (g *GCS) Flush() {
	g.logger.Info("Flushing GCS")
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.bytesCounter == 0 {
		g.logger.Info("buffer is empty, skip flushing")
		return
	}

	// TODO: retry indefinitely
	err := g.uploadFunc()
	if err != nil {
		g.logger.Error(err)
		return
	}
	g.logger.Warn("flushed to GCS")

	g.logger.Warn("calling onFlushFunc...")
	for _, f := range g.onFlushFunc {
		err := f()
		if err != nil {
			g.logger.Error(err.Error())
		}
	}
	g.logger.Warn("onFlushFuncs called")

	// close the current buffer, and create new one
	g.logger.Debug("recreating the buffer")
	g.buffer.Close()
	g.bytesCounter = 0
	for {
		g.buffer, err = NewFileBuffer(g.name)
		if err == nil {
			break
		}
		g.logger.Error(fmt.Errorf("Failed to create new buffer: %s", err))
		time.Sleep(time.Second)
	}
	g.logger.Debug("buffer recreated")

}

type FileBuffer struct {
	f *os.File
}

func NewFileBuffer(name string) (*FileBuffer, error) {
	file, err := os.CreateTemp(os.TempDir(), fmt.Sprintf("consumer-buffer-%s", name))
	if err != nil {
		return nil, err
	}
	return &FileBuffer{f: file}, nil
}

func (f *FileBuffer) Read(b []byte) (n int, err error) {
	panic("should not be called")
}

// can't used same file handler for read and write,
// because of offset
func (f *FileBuffer) WriteTo(w io.Writer) (n int64, err error) {
	temp, err := os.Open(f.f.Name())
	if err != nil {
		return 0, err
	}
	return io.Copy(w, temp)
}

// TODO: use bufio
func (f *FileBuffer) Write(b []byte) (n int, err error) {
	return f.f.Write(b)
}

func (f *FileBuffer) Close() error {
	f.f.Close()
	return os.Remove(f.f.Name())
}
