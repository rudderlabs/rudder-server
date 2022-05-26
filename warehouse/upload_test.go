package warehouse

import (
	"errors"
	"testing"
)

func TestExtractUploadErrorsByState(t *testing.T) {

	input := []struct {
		InitialErrorState []byte
		CurrentErrorState string
		CurrentError      error
		ErrorCount        int
	}{
		{
			InitialErrorState: []byte(`{}`),
			CurrentErrorState: InternalProcessingFailed,
			CurrentError:      errors.New("account locked"),
			ErrorCount:        1,
		},
		{
			InitialErrorState: []byte(`{"internal_processing_failed": {"errors": ["account locked"], "attempt": 1}}`),
			CurrentErrorState: InternalProcessingFailed,
			CurrentError:      errors.New("account locked again"),
			ErrorCount:        2,
		},
		{
			InitialErrorState: []byte(`{"internal_processing_failed": {"errors": ["account locked", "account locked again"], "attempt": 2}}`),
			CurrentErrorState: TableUploadExportingFailed,
			CurrentError:      errors.New("failed to load data because failed in earlier job"),
			ErrorCount:        1,
		},
	}

	for _, ip := range input {

		uploadErrors, err := extractAndUpdateUploadErrorsByState(ip.InitialErrorState, ip.CurrentErrorState, ip.CurrentError)
		if err != nil {
			t.Errorf("extracting upload errors by state should have passed: %v", err)
		}

		stateErrors := uploadErrors[ip.CurrentErrorState]
		// Below switch clause mirrors how we are
		// adding data in generic interface.

		var errorLength int
		switch stateErrors["errors"].(type) {
		case []string:
			errorLength = len(stateErrors["errors"].([]string))
		case []interface{}:
			errorLength = len(stateErrors["errors"].([]interface{}))
		}

		if errorLength != ip.ErrorCount {
			t.Errorf("expected error to be addded to list of state errors")
		}

		if stateErrors["attempt"].(int) != ip.ErrorCount {
			t.Errorf("expected attempts to be: %d, got: %d", ip.ErrorCount, stateErrors["attempt"].(int))
		}
	}

}
