package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/profiler"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jsonrs"
	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	wtrans "github.com/rudderlabs/rudder-server/warehouse/transformer"
)

const (
	processorTransformer = "PROCESSOR_TRANSFORMER"
	warehouseTransformer = "WAREHOUSE_TRANSFORMER"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGKILL, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	sampleEvent := mustString("SAMPLE_EVENT")
	eventsInBatch, batchSize := mustInt("EVENTS_IN_BATCH"), mustInt("BATCH_SIZE")
	iterations := mustInt("NUM_OF_ITERATIONS")
	mode := mustString("MODE")

	if eventsInBatch < batchSize {
		return errors.New("eventsInBatch must be greater than or equal to batchSize")
	}

	runID := uuid.New().String()
	conf := config.Default
	conf.Set("Processor.transformBatchSize", batchSize)

	logFactory := logger.Default
	l := logFactory.NewLogger().Child("warehouse-transformer-benchmark").Child(mode)

	var te types.TransformerEvent
	err := jsonrs.Unmarshal([]byte(sampleEvent), &te)
	if err != nil {
		return fmt.Errorf("could not unmarshal sample client event: %w", err)
	}

	clientEvents := lo.RepeatBy(eventsInBatch, func(int) types.TransformerEvent {
		return te
	})

	t, err := selectTransformer(mode, conf, l)
	if err != nil {
		return fmt.Errorf("could not select transformer: %w", err)
	}

	startTime := timeutil.Now()

	l.Infon("Starting transformer")
	l.Infof("Started At: %s", startTime.Format(time.RFC3339))
	l.Infof("Sample Event: %s", sampleEvent)
	l.Infof("Events in Batch: %d", eventsInBatch)
	l.Infof("Batch size: %d", batchSize)
	l.Infof("Iterations: %d", iterations)
	l.Infof("Mode: %s", mode)
	l.Infof("RunID: %s", runID)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return profiler.StartServer(gCtx, config.GetInt("Profiler.Port", 7777))
	})
	g.Go(func() error {
		for i := 0; i < iterations; i++ {
			l.Infof("Running iteration: %d", i+1)
			t.Transform(gCtx, clientEvents)
		}

		endTime := timeutil.Now().Sub(startTime).Seconds()

		l.Infof("Total events transformer: %d", len(clientEvents)*iterations)
		l.Infof("Total time for transformations: %v", endTime)
		l.Infof("Transformation rate (msg/s): %v",
			float64(len(clientEvents)*iterations)/endTime,
		)
		l.Infof("Finished transformer: %v", endTime)

		cancel()
		return nil
	})
	return g.Wait()
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
) (ptrans.DestinationClient, error) {
	switch strings.ToUpper(mode) {
	case processorTransformer:
		return ptrans.NewClients(conf, l, stats.NOP).Destination(), nil
	case warehouseTransformer:
		return wtrans.New(conf, l, stats.NOP), nil
	default:
		return nil, fmt.Errorf("invalid mode: %s", mode)
	}
}
