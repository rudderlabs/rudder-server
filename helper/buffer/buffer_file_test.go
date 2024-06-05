package buffer_test

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/helper/buffer"
	"github.com/rudderlabs/rudder-server/helper/types"
)

func TestSend(t *testing.T) {
	buf := buffer.New("module")

	buf.Send(`{"request":{"url": "http://google.com"}}`, `{"response":{"body":"i have come to you"}}`, types.MetaInfo{
		DestinationID: "desId",
		WorkspaceID:   "wspId",
		DestType:      "ABC",
	})

	buf.Shutdown()
}

type reqResWithMeta struct {
	req  string
	res  string
	meta types.MetaInfo
}

func TestSendForMultipleGoRoutines(t *testing.T) {
	totalGoRoutines := 10
	var wg sync.WaitGroup
	conf := config.New()
	conf.Set("module.DebugHelper.bufferCapacityInB", 200)
	conf.Set("module.DebugHelper.maxBytesForFileRotation", 20000)
	commonMeta := types.MetaInfo{
		DestinationID: "desId",
		WorkspaceID:   "wspId",
		DestType:      "ABC",
	}
	tempDir := t.TempDir()
	buf := buffer.New("module", buffer.WithDirectory(tempDir), buffer.WithOptsFromConfig("module", conf))
	isExist, err := exists(tempDir + "/buffer_file_debug_log_0.jsonl")
	require.NoError(t, err)
	require.Equal(t, true, isExist)

	t.Cleanup(func() {
		buf.Shutdown()
	})

	for i := 0; i < totalGoRoutines; i++ {
		wg.Add(1)
		workerId := strconv.Itoa(i)

		events := lo.Map(lo.Range(30), func(i, _ int) reqResWithMeta {
			return reqResWithMeta{
				req:  fmt.Sprintf(`{"request":{"url": "http://website-%d.com","headers":{"worker":%s}}}`, i, workerId),
				res:  fmt.Sprintf(`{"response":{"body":"i have come to you-%d","worker":%s}}`, i, workerId),
				meta: commonMeta,
			}
		})
		go func() {
			lo.ForEach(events, func(reqResMeta reqResWithMeta, _ int) {
				buf.Send(reqResMeta.req, reqResMeta.res, reqResMeta.meta)
			})
			wg.Done()
		}()
	}
	wg.Wait()
	nFiles, err := countFiles(tempDir)
	require.NoError(t, err)
	require.Equal(t, 4, nFiles)
}

func countFiles(dir string) (int, error) {
	f, err := os.Open(dir)
	if err != nil {
		return 0, err
	}
	defer f.Close() // Close the file handle when done

	files, err := f.Readdirnames(0)
	if err != nil {
		return 0, err
	}
	return len(files), nil
}

func exists(name string) (bool, error) {
	_, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}
