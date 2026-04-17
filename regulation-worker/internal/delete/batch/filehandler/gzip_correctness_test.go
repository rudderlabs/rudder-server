package filehandler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

// removeFn adapts all three implementations to a common signature so we can
// drive them from a single table-driven test.
type removeFn func(ctx context.Context, casing Case, records []byte, users []model.User) ([]byte, error)

func runRemoveIdentity(ctx context.Context, casing Case, records []byte, users []model.User) ([]byte, error) {
	h := NewGZIPLocalFileHandler(casing)
	h.records = append([]byte(nil), records...)
	if err := h.RemoveIdentity(ctx, users); err != nil {
		return nil, err
	}
	return h.records, nil
}

func runRemoveIdentityRE(ctx context.Context, casing Case, records []byte, users []model.User) ([]byte, error) {
	h := NewGZIPLocalFileHandler(casing)
	h.records = append([]byte(nil), records...)
	if err := h.RemoveIdentityRE(ctx, users); err != nil {
		return nil, err
	}
	return h.records, nil
}

func runRemoveIdentityPureGo(ctx context.Context, casing Case, records []byte, users []model.User) ([]byte, error) {
	h := NewGZIPLocalFileHandler(casing)
	h.records = append([]byte(nil), records...)
	if err := h.RemoveIdentityPureGo(ctx, users); err != nil {
		return nil, err
	}
	return h.records, nil
}

var allImpls = []struct {
	name string
	fn   removeFn
}{
	{"RemoveIdentity", runRemoveIdentity},
	{"RemoveIdentityRE", runRemoveIdentityRE},
	{"RemoveIdentityPureGo", runRemoveIdentityPureGo},
}

// TestRemoveIdentity_SharedCorrectness runs inputs that all three
// implementations must agree on. Keep these cases "flat" — no nested
// occurrences of the id key — since the regex/sed impls cannot distinguish
// nested from top-level matches.
func TestRemoveIdentity_SharedCorrectness(t *testing.T) {
	cases := []struct {
		name     string
		casing   Case
		users    []model.User
		input    string
		expected string
	}{
		{
			name:     "empty input",
			casing:   SnakeCase,
			users:    []model.User{{ID: "a"}},
			input:    "",
			expected: "",
		},
		{
			name:     "no match keeps everything",
			casing:   SnakeCase,
			users:    []model.User{{ID: "missing"}},
			input:    `{"user_id": "a"}` + "\n" + `{"user_id": "b"}` + "\n",
			expected: `{"user_id": "a"}` + "\n" + `{"user_id": "b"}` + "\n",
		},
		{
			name:     "single match drops the line (snake_case)",
			casing:   SnakeCase,
			users:    []model.User{{ID: "a"}},
			input:    `{"user_id": "a"}` + "\n" + `{"user_id": "b"}` + "\n",
			expected: `{"user_id": "b"}` + "\n",
		},
		{
			name:     "single match drops the line (camelCase)",
			casing:   CamelCase,
			users:    []model.User{{ID: "a"}},
			input:    `{"userId": "a"}` + "\n" + `{"userId": "b"}` + "\n",
			expected: `{"userId": "b"}` + "\n",
		},
		{
			name:     "single match drops the line (UPPER_CASE)",
			casing:   UpperCase,
			users:    []model.User{{ID: "a"}},
			input:    `{"USER_ID": "a"}` + "\n" + `{"USER_ID": "b"}` + "\n",
			expected: `{"USER_ID": "b"}` + "\n",
		},
		{
			name:   "multiple suppressed ids",
			casing: SnakeCase,
			users: []model.User{
				{ID: "a"},
				{ID: "c"},
			},
			input:    `{"user_id": "a"}` + "\n" + `{"user_id": "b"}` + "\n" + `{"user_id": "c"}` + "\n",
			expected: `{"user_id": "b"}` + "\n",
		},
		{
			name:     "drops all lines",
			casing:   SnakeCase,
			users:    []model.User{{ID: "a"}, {ID: "b"}},
			input:    `{"user_id": "a"}` + "\n" + `{"user_id": "b"}` + "\n",
			expected: "",
		},
		{
			name:     "partial id is not matched",
			casing:   CamelCase,
			users:    []model.User{{ID: "valid-user-id"}},
			input:    `{"userId": "invalid-user-id"}` + "\n",
			expected: `{"userId": "invalid-user-id"}` + "\n",
		},
		{
			name:     "id with regex metacharacters matches exactly",
			casing:   SnakeCase,
			users:    []model.User{{ID: "my-.-user-id"}},
			input:    `{"user_id": "my-.-user-id"}` + "\n" + `{"user_id": "my-X-user-id"}` + "\n",
			expected: `{"user_id": "my-X-user-id"}` + "\n",
		},
		{
			name:     "id with shell metacharacters matches exactly",
			casing:   CamelCase,
			users:    []model.User{{ID: "!@#$%^&*()***"}},
			input:    `{"userId": "!@#$%^&*()***", "event": "track"}` + "\n" + `{"userId": "other"}` + "\n",
			expected: `{"userId": "other"}` + "\n",
		},
		{
			name:     "extra whitespace between key and value",
			casing:   SnakeCase,
			users:    []model.User{{ID: "a"}},
			input:    `{"user_id":    "a"}` + "\n" + `{"user_id": "b"}` + "\n",
			expected: `{"user_id": "b"}` + "\n",
		},
		{
			name:     "record with additional fields is dropped when id matches",
			casing:   SnakeCase,
			users:    []model.User{{ID: "a"}},
			input:    `{"user_id": "a", "event": "track", "properties": {"page": "home"}}` + "\n",
			expected: "",
		},
	}

	ctx := context.Background()
	for _, tc := range cases {
		for _, impl := range allImpls {
			t.Run(impl.name+"/"+tc.name, func(t *testing.T) {
				got, err := impl.fn(ctx, tc.casing, []byte(tc.input), tc.users)
				require.NoError(t, err)
				require.Equal(t, tc.expected, string(got))
			})
		}
	}
}

// TestRemoveIdentity_UnsupportedCasing asserts every impl rejects an
// unrecognised casing.
func TestRemoveIdentity_UnsupportedCasing(t *testing.T) {
	ctx := context.Background()
	for _, impl := range allImpls {
		t.Run(impl.name, func(t *testing.T) {
			_, err := impl.fn(ctx, Case(99), []byte(`{"user_id": "a"}`+"\n"), []model.User{{ID: "a"}})
			require.Error(t, err)
		})
	}
}

// TestRemoveIdentity_EmptyUsersBehaviour documents that the sed-based
// RemoveIdentity errors out when given an empty users list (it builds an
// invalid `sed -r -e` command), while the regex and pure-Go impls correctly
// pass the input through unchanged.
func TestRemoveIdentity_EmptyUsersBehaviour(t *testing.T) {
	ctx := context.Background()
	input := []byte(`{"user_id": "a"}` + "\n" + `{"user_id": "b"}` + "\n")

	t.Run("RemoveIdentity errors", func(t *testing.T) {
		_, err := runRemoveIdentity(ctx, SnakeCase, input, nil)
		require.Error(t, err)
	})
	t.Run("RemoveIdentityRE passes through", func(t *testing.T) {
		got, err := runRemoveIdentityRE(ctx, SnakeCase, input, nil)
		require.NoError(t, err)
		require.Equal(t, string(input), string(got))
	})
	t.Run("RemoveIdentityPureGo passes through", func(t *testing.T) {
		got, err := runRemoveIdentityPureGo(ctx, SnakeCase, input, nil)
		require.NoError(t, err)
		require.Equal(t, string(input), string(got))
	})
}

// TestRemoveIdentityPureGo_IgnoresNestedKeys documents a behavioural
// difference: PureGo parses JSON and only inspects the top-level id field, so
// a matching id nested under another object does not cause the record to be
// dropped. The regex/sed impls match textually and would drop such a record.
func TestRemoveIdentityPureGo_IgnoresNestedKeys(t *testing.T) {
	input := []byte(`{"user_id": "keep", "context": {"user_id": "drop"}}` + "\n")
	got, err := runRemoveIdentityPureGo(t.Context(), SnakeCase, input, []model.User{{ID: "drop"}})
	require.NoError(t, err)
	require.Equal(t, string(input), string(got))
}

// TestRemoveIdentityPureGo_PreservesInputFormat documents that in-place
// compaction preserves the input faithfully — if the final line lacks a
// trailing newline, the output does too.
func TestRemoveIdentityPureGo_PreservesInputFormat(t *testing.T) {
	input := []byte(`{"user_id": "a"}` + "\n" + `{"user_id": "b"}`)
	got, err := runRemoveIdentityPureGo(t.Context(), SnakeCase, input, []model.User{{ID: "a"}})
	require.NoError(t, err)
	require.Equal(t, `{"user_id": "b"}`, string(got))
}
