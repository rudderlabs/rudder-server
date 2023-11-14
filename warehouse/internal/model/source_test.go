package model

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFromSourceJobType(t *testing.T) {
	testCases := []struct {
		name     string
		jobType  string
		expected SourceJobType
		wantErr  error
	}{
		{
			name:     "delete bv job run id",
			jobType:  "deletebyjobrunid",
			expected: SourceJobTypeDeleteByJobRunID,
			wantErr:  nil,
		},
		{
			name:     "invalid",
			jobType:  "invalid",
			expected: nil,
			wantErr:  fmt.Errorf("invalid job type %s", "invalid"),
		},
		{
			name:     "empty",
			jobType:  "",
			expected: nil,
			wantErr:  fmt.Errorf("invalid job type %s", ""),
		},
		{
			name:     "nil",
			jobType:  "",
			expected: nil,
			wantErr:  fmt.Errorf("invalid job type %s", ""),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			jobType, err := FromSourceJobType(tc.jobType)
			if tc.wantErr != nil {
				require.Equal(t, tc.wantErr, err)
				require.Nil(t, jobType)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, jobType)
		})
	}
}

func TestFromSourceJobStatus(t *testing.T) {
	testCases := []struct {
		name      string
		status    string
		expected  SourceJobStatus
		wantError error
	}{
		{
			name:      "waiting",
			status:    "waiting",
			expected:  SourceJobStatusWaiting,
			wantError: nil,
		},
		{
			name:      "executing",
			status:    "executing",
			expected:  SourceJobStatusExecuting,
			wantError: nil,
		},
		{
			name:      "failed",
			status:    "failed",
			expected:  SourceJobStatusFailed,
			wantError: nil,
		},
		{
			name:      "aborted",
			status:    "aborted",
			expected:  SourceJobStatusAborted,
			wantError: nil,
		},
		{
			name:      "succeeded",
			status:    "succeeded",
			expected:  SourceJobStatusSucceeded,
			wantError: nil,
		},
		{
			name:      "invalid",
			status:    "invalid",
			expected:  nil,
			wantError: fmt.Errorf("invalid job status %s", "invalid"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			jobStatus, err := FromSourceJobStatus(tc.status)
			if tc.wantError != nil {
				require.Equal(t, tc.wantError, err)
				require.Nil(t, jobStatus)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, jobStatus)
		})
	}
}
