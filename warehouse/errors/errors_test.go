package errors_test

import (
	"bufio"
	"errors"
	errors2 "github.com/rudderlabs/rudder-server/warehouse/errors"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/stretchr/testify/require"
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

			m, err := manager.New(destType)
			require.NoError(t, err)

			er := &errors2.ErrorHandler{Manager: m}

			f, err := os.Open(file)
			require.NoError(t, err)

			defer func() { _ = f.Close() }()

			uploadsErrors, err := readLines(f)
			require.NoError(t, err)

			for _, uploadError := range uploadsErrors {
				tag := er.MatchErrorMappings(errors.New(uploadError))
				require.Equal(t, tag.Name, "error_mapping")
				require.NotContains(t, tag.Value, string(model.UnknownError))
			}
		})

		t.Run("UnKnown errors: "+destType, func(t *testing.T) {
			t.Parallel()

			m, err := manager.New(destType)
			require.NoError(t, err)

			er := &errors2.ErrorHandler{Manager: m}
			tag := er.MatchErrorMappings(errors.New("unknown error"))
			require.Equal(t, tag.Name, "error_mapping")
			require.Contains(t, tag.Value, string(model.UnknownError))
		})
	}
}
