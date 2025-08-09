package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"net/http"

	"github.com/BaritoLog/barito-flow/cmds"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"

	_ "net/http/pprof"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	Name    = "barito-flow"
	Version = "0.13.5"
)

var (
	Commit string = "N/A"
	Build  string = "MANUAL"
)

func init() {
	log.SetLevel(log.WarnLevel)
}

func main() {
	app := cli.App{
		Name:    Name,
		Usage:   "Provide kafka producer or consumer for Barito project",
		Version: fmt.Sprintf("%s-%s-%s", Version, Build, Commit),
		Commands: []cli.Command{
			{
				Name:      "producer",
				ShortName: "p",
				Usage:     "start barito-flow as producer",
				Action:    cmds.ActionBaritoProducerService,
				Flags: []cli.Flag{
					cli.BoolFlag{
						Name:  "verbose, V",
						Usage: "Enable verbose mode",
					},
				},
			},
			{
				Name:      "consumer",
				ShortName: "c",
				Usage:     "start barito-flow as consumer",
				Action:    cmds.ActionBaritoConsumerService,
				Flags: []cli.Flag{
					cli.BoolFlag{
						Name:  "verbose, V",
						Usage: "Enable verbose mode",
					},
				},
			},
			{
				Name:      "consumer for GCS",
				ShortName: "consumer-gcs",
				Usage:     "start barito-flow as consumer for GCS",
				Action:    cmds.ActionBaritoConsumerGCSService,
				Flags: []cli.Flag{
					cli.BoolFlag{
						Name:  "verbose, V",
						Usage: "Enable verbose mode",
					},
				},
			},
		},
		UsageText: "barito-flow [commands]",
		Before: func(c *cli.Context) error {
			fmt.Fprintf(os.Stderr, "%s Started. Version: %s Build: %s Commit: %s\n", Name, Version, Build, Commit)
			return nil
		},
		CommandNotFound: func(c *cli.Context, command string) {
			fmt.Fprintf(os.Stderr, "Command not found: '%s'. Please use '%s -h' to view usage\n", command, Name)
		},
	}

	shutdown, err := setupOTelSDK(context.Background())
	if err != nil {
		log.Fatalf("Failed to set up OpenTelemetry SDK: %v", err)
	}
	defer func() {
		shutdown(context.Background())
	}()

	http.Handle("/metrics", promhttp.Handler())
	exporterPort, exists := os.LookupEnv("EXPORTER_PORT")
	if !exists {
		exporterPort = ":8008"
	}
	go http.ListenAndServe(exporterPort, nil)

	err = app.Run(os.Args)
	if err != nil {
		log.Fatalf("Some error occurred: %s", err.Error())
	}
}

// setupOTelSDK bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func setupOTelSDK(ctx context.Context) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error

	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	// handleErr calls shutdown for cleanup and makes sure that all errors are returned.
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	// Set up propagator.
	prop := newPropagator()
	otel.SetTextMapPropagator(prop)

	// Set up trace provider.
	tracerProvider, err := newTracerProvider()
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	return
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func newTracerProvider() (*trace.TracerProvider, error) {
	traceExporter, err := otlptrace.New(
		context.Background(),
		otlptracegrpc.NewClient(
			otlptracegrpc.WithEndpoint("localhost:4317"),
			otlptracegrpc.WithInsecure(),
		),
	)
	if err != nil {
		return nil, err
	}

	tracerProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter,
			// Default is 5s. Set to 1s for demonstrative purposes.
			trace.WithBatchTimeout(time.Second)),
	)
	return tracerProvider, nil
}
