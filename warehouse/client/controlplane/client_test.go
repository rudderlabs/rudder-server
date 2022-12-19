package controlplane_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	cp "github.com/rudderlabs/rudder-server/warehouse/client/controlplane"
	"github.com/stretchr/testify/require"
)

func TestFetchSSHKeys(t *testing.T) {
	t.Log("running tests to fetch the ssh keys")

	testcases := []struct {
		name            string
		responseBody    string
		responseCode    int
		destinationID   string
		expectedError   error
		expectedKeyPair *cp.PublicPrivateKeyPair
	}{
		{
			name:          "fetch ssh keys returns the keys correctly",
			responseBody:  `{"publicKey": "public_key", "privateKey": "private_key"}`,
			responseCode:  http.StatusOK,
			destinationID: "id",
			expectedError: nil,
			expectedKeyPair: &cp.PublicPrivateKeyPair{
				PublicKey:  "public_key",
				PrivateKey: "private_key",
			},
		},
		{
			name:            "fetch ssh keys returns no keys found",
			responseBody:    ``,
			responseCode:    http.StatusNotFound,
			destinationID:   "id",
			expectedError:   fmt.Errorf("%w: key requested: id", cp.ErrKeyNotFound),
			expectedKeyPair: nil,
		},
		{
			name:            "fetch ssh keys fails unexpectedly",
			responseBody:    ``,
			responseCode:    http.StatusInternalServerError,
			destinationID:   "id",
			expectedError:   errors.New("invalid status code: 500"),
			expectedKeyPair: nil,
		},
	}

	for _, tc := range testcases {

		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			svc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _, ok := r.BasicAuth()
				require.True(t, ok)
				require.Equal(t, http.MethodGet, r.Method)
				require.Equal(t, "application/json", r.Header.Get("Content-Type"))

				w.WriteHeader(tc.responseCode)
				w.Write([]byte(tc.responseBody))
			}))

			defer svc.Close()

			client := cp.NewInternalClient(svc.URL, cp.BasicAuth{
				Username: "username",
				Password: "password",
			})

			keys, err := client.GetDestinationSSHKeys(context.TODO(), tc.destinationID)
			require.Equal(t, tc.expectedError, err)
			require.Equal(t, tc.expectedKeyPair, keys)
		})
	}
}
