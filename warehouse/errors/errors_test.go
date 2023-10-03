package errors_test

import (
	"bufio"
	"errors"
	"os"
	"path"
	"path/filepath"
	"testing"

	werrors "github.com/rudderlabs/rudder-server/warehouse/errors"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
)

func TestErrorHandler_MatchErrorMappings(t *testing.T) {
	readLines := func(f *os.File) ([]string, error) {
		var (
			lines []string

			scanner = bufio.NewScanner(f)
		)

		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			lines = append(lines, line)
		}
		return lines, scanner.Err()
	}

	var (
		files []string
		err   error
	)

	files, err = filepath.Glob("testdata/errors/**")
	require.NoError(t, err)

	for _, file := range files {
		file := file
		_, destType := path.Split(file)

		t.Run("Known errors: "+destType, func(t *testing.T) {
			t.Parallel()

			m, err := manager.New(destType, config.Default, logger.NOP, stats.Default)
			require.NoError(t, err)

			er := &werrors.ErrorHandler{Manager: m}

			f, err := os.Open(file)
			require.NoError(t, err)

			defer func() { _ = f.Close() }()

			uploadsErrors, err := readLines(f)
			require.NoError(t, err)

			for _, uploadError := range uploadsErrors {
				tag := er.MatchErrorMappings(errors.New(uploadError))
				require.Equal(t, tag.Name, "error_mapping")
				require.NotContains(t, tag.Value, string(model.UnknownError))
				require.NotContains(t, tag.Value, string(model.Noop))
			}
		})

		t.Run("UnKnown errors: "+destType, func(t *testing.T) {
			t.Parallel()

			m, err := manager.New(destType, config.Default, logger.NOP, stats.Default)
			require.NoError(t, err)

			er := &werrors.ErrorHandler{Manager: m}
			tag := er.MatchErrorMappings(errors.New("unknown error"))
			require.Equal(t, tag.Name, "error_mapping")
			require.Equal(t, tag.Value, string(model.UnknownError))
		})

		t.Run("Nil manager: "+destType, func(t *testing.T) {
			t.Parallel()

			er := &werrors.ErrorHandler{Manager: nil}
			tag := er.MatchErrorMappings(errors.New("unknown error"))
			require.Equal(t, tag.Name, "error_mapping")
			require.Equal(t, tag.Value, string(model.Noop))
		})

		t.Run("Nil error: "+destType, func(t *testing.T) {
			t.Parallel()

			er := &werrors.ErrorHandler{Manager: nil}
			tag := er.MatchErrorMappings(errors.New("unknown error"))
			require.Equal(t, tag.Name, "error_mapping")
			require.Equal(t, tag.Value, string(model.Noop))
		})
	}
}
