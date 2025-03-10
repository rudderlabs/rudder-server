package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	svcMetric "github.com/rudderlabs/rudder-go-kit/stats/metric"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/jsonrs"
	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	wtrans "github.com/rudderlabs/rudder-server/warehouse/transformer"
)

var commit string

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGKILL, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func Run(ctx context.Context) error {
	conf := config.Default

	logFactory := logger.Default
	l := logFactory.NewLogger().Child("warehouse-transformer-benchmark")

	stats.Default = stats.NewStats(conf, logFactory, svcMetric.Instance, []stats.Option{
		stats.WithServiceName("warehouse-transformer-benchmark"),
		stats.WithServiceVersion(commit),
	}...)
	if err := stats.Default.Start(ctx, rruntime.GoRoutineFactory); err != nil {
		return fmt.Errorf("could not start stats: %w", err)
	}
	defer func() {
		stats.Default.Stop()
	}()

	sampleEvent := mustString("SAMPLE_CLIENT_EVENT")
	eventsInBatch := mustInt("EVENTS_IN_BATCH")
	batchSize := mustInt("BATCH_SIZE")
	iterations := mustInt("ITERATIONS")
	mode := mustString("MODE")

	var transformerEvent types.TransformerEvent
	err := jsonrs.Unmarshal([]byte(sampleEvent), &transformerEvent)
	if err != nil {
		return fmt.Errorf("could not unmarshal sample client event: %w", err)
	}

	clientEvents := lo.RepeatBy(eventsInBatch, func(index int) types.TransformerEvent {
		return transformerEvent
	})

	start := time.Now()

	log.Println("Starting transformer")
	log.Println("Started At", start.Format(time.RFC3339))
	log.Println("Batch size:", batchSize)
	log.Println("Iterations:", iterations)
	log.Println("Mode:", mode)
	log.Println("Client events:", len(clientEvents))
	defer func() {
		log.Println("Finished transformer", time.Since(start))
	}()

	t, err := selectTransformer(mode, conf, l)
	if err != nil {
		return fmt.Errorf("could not select transformer: %w", err)
	}

	for i := 0; i < iterations; i++ {
		log.Println("Running iteration", i+1)
		executeTransformation(ctx, t, clientEvents, batchSize, mode)
	}
	return nil
}

func mustInt(s string) int {
	i, err := strconv.Atoi(os.Getenv(s))
	if err != nil {
		panic(fmt.Errorf("invalid int: %s: %v", s, err))
	}
	return i
}

func mustString(s string) string {
	v := os.Getenv(s)
	if v == "" {
		panic(fmt.Errorf("invalid string: %s", s))
	}
	return v
}

func selectTransformer(
	mode string,
	conf *config.Config,
	l logger.Logger,
) (ptrans.DestinationTransformer, error) {
	switch mode {
	case "PROCESSOR_TRANSFORMATIONS":
		return ptrans.NewTransformer(conf, l, stats.Default), nil
	case "WAREHOUSE_TRANSFORMATIONS":
		return wtrans.New(conf, l, stats.Default), nil
	default:
		return nil, fmt.Errorf("invalid mode: %s", mode)
	}
}

func executeTransformation(
	ctx context.Context,
	t ptrans.DestinationTransformer,
	clientEvents []types.TransformerEvent,
	batchSize int,
	mode string,
) {
	startTime := timeutil.Now()
	res := t.Transform(ctx, clientEvents, batchSize)

	tags := stats.Tags{"commit": commit, "mode": mode}
	stats.Default.NewTaggedStat("warehouse_transformer_benchmark_request_latency", stats.TimerType, tags).Since(startTime)
	stats.Default.NewTaggedStat("warehouse_transformer_benchmark_requests", stats.CountType, tags).Increment()
	stats.Default.NewTaggedStat("warehouse_transformer_benchmark_input_events", stats.HistogramType, tags).Observe(float64(len(clientEvents)))
	stats.Default.NewTaggedStat("warehouse_transformer_benchmark_output_events", stats.HistogramType, tags).Observe(float64(len(res.Events)))
	stats.Default.NewTaggedStat("warehouse_transformer_benchmark_output_failed_events", stats.HistogramType, tags).Observe(float64(len(res.FailedEvents)))
}
