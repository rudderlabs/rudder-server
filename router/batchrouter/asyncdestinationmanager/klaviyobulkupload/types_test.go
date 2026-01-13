package klaviyobulkupload

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorDetailList(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		t.Run("with elements", func(t *testing.T) {
			l := ErrorDetailList{
				{
					ID:     "1",
					Code:   "400",
					Title:  "Invalid data",
					Detail: "Email is missing",
					Source: ErrorSource{
						Pointer:   "/data/attributes/email",
						Parameter: "email",
					},
				},
				{
					ID:     "2",
					Code:   "401",
					Title:  "Unauthorized",
					Detail: "API key is invalid",
					Source: ErrorSource{
						Pointer:   "/data/attributes/api_key",
						Parameter: "api_key",
					},
				},
			}
			expected := "{ID=1, Code=400, Title=Invalid data, Detail=Email is missing, Source={Pointer=/data/attributes/email, Parameter=email}},{ID=2, Code=401, Title=Unauthorized, Detail=API key is invalid, Source={Pointer=/data/attributes/api_key, Parameter=api_key}}"
			require.Equal(t, expected, l.String())
		})

		t.Run("empty list", func(t *testing.T) {
			l := ErrorDetailList{}
			require.Equal(t, "", l.String())
		})
		t.Run("nil list", func(t *testing.T) {
			var l ErrorDetailList
			require.Equal(t, "", l.String())
		})
	})
}
