package flow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

type DummyUploadFunc struct {
	called int
}

func (d *DummyUploadFunc) Upload() error {
	d.called++
	return nil
}

type DummyClock struct {
	now time.Time
}

func (d *DummyClock) Now() time.Time { return d.now }

func TestGCS_flushMaxBytes(t *testing.T) {
	// should call flush when the buffer is more than flushMaxBytes
	uploadFunc := &DummyUploadFunc{}
	g := newTestGCS()
	g.flushMaxBytes = 30
	g.uploadFunc = uploadFunc.Upload
	go g.Start()

	payload := []byte("12345678901")
	g.OnMessage(payload)
	require.Empty(t, uploadFunc.called, "should not upload yet")
	g.OnMessage(payload)
	require.Empty(t, uploadFunc.called, "should not upload yet")
	g.OnMessage(payload)
	time.Sleep(1 * time.Second)
	require.Equal(t, 1, uploadFunc.called, "should upload already")

}

func TestGCS_flushMaxTime(t *testing.T) {
	// should call flush when max time reached
	uploadFunc := &DummyUploadFunc{}
	g := newTestGCS()
	g.flushMaxTime = 1 * time.Second
	g.uploadFunc = uploadFunc.Upload
	require.Equal(t, 0, uploadFunc.called, "should not upload yet")
	g.OnMessage([]byte("12"))
	go g.Start()
	time.Sleep(2 * time.Second)
	require.Equal(t, 1, uploadFunc.called, "should upload already")
}

func TestGCS_onMessage(t *testing.T) {
	// should return error when buffer is full
	g := newTestGCS()
	g.flushMaxBytes = 10
	g.OnMessage([]byte("1234567890"))
	err := g.OnMessage([]byte("1234567890"))
	require.Error(t, err)
}

func TestGGS_Flush(t *testing.T) {
	t.Run("should call onFlushFunc when uploadFunc success ", func(t *testing.T) {
		g := newTestGCS()
		g.uploadFunc = func() error { return nil }
		g.OnMessage([]byte("foo"))

		called := 0
		for i := 0; i < 10; i++ {
			g.AddOnFlushFunc(func() error {
				called++
				return nil
			})
		}
		g.Flush()
		require.Equal(t, 10, called, "should be called 10 times")
	})

	t.Run("should NOT call onFlushFunc when uploadFunc failed ", func(t *testing.T) {
		g := newTestGCS()
		g.uploadFunc = func() error { return errors.New("foo") }
		g.OnMessage([]byte("foo"))

		called := 0
		for i := 0; i < 10; i++ {
			g.AddOnFlushFunc(func() error {
				called++
				return nil
			})
		}
		g.Flush()
		require.Equal(t, 0, called, "should not be called")
		n, err := g.buffer.Read([]byte{})
		require.NoError(t, err)
		require.Equal(t, 0, n, "buffer should be empty after flush")

	})
}

func TestGCS_uploadToGCS(t *testing.T) {
	ctx := context.TODO()
	clock := &DummyClock{now: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)}

	var called int
	var path, queryString string
	var payload []byte

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called++
		path = r.URL.Path
		queryString = r.URL.RawQuery
		payload, _ = io.ReadAll(r.Body)
		defer r.Body.Close()
	}))
	defer ts.Close()

	g := newTestGCS()
	g.OnMessage([]byte("test1"))
	g.OnMessage([]byte("test2"))
	g.OnMessage([]byte("test3"))

	g.storageClient, _ = storage.NewClient(ctx, option.WithoutAuthentication(), option.WithEndpoint(ts.URL))
	g.name = "authorization-service"
	g.bucketName = "foo"
	g.bucketPath = "/bar"
	g.clock = clock
	g.uploadToGCS()

	require.Equal(t, 1, called, "should be called")
	require.Equal(t, "/upload/storage/v1/b/foo/o", path, "should be called with correct path")
	//require.Contains(t, string(payload), "test1\ntest2\ntest3", "should be called with correct payload")
	require.Contains(t, queryString, fmt.Sprintf(`name=%%2Fbar%%2Fauthorization-service%%2Fauthorization-service-%s.log`, url.QueryEscape(g.clock.Now().Format(time.RFC3339))), "should be called with correct query string")
}

func newTestGCS() *GCS {
	buffer, _ := NewFileBuffer("test")
	g := &GCS{
		name:          "test",
		flushMaxTime:  1 * time.Minute,
		flushMaxBytes: 100,
		buffer:        buffer,
		onFlushFunc:   []func() error{},
		logger:        log.New(),
	}
	g.uploadFunc = g.uploadToGCS
	return g
}
