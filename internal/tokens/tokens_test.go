package tokens

import "testing"

func TestIsSubsetMatch(t *testing.T) {
	for _, test := range []struct {
		subject string
		test    string
		result  bool
	}{
		{"foo.bar", "foo.bar", true},
		{"foo.*", ">", true},
		{"foo.*", "*.*", true},
		{"foo.*", "foo.*", true},
		{"foo.*", "foo.bar", false},
		{"foo.>", ">", true},
		{"foo.>", "*.>", true},
		{"foo.>", "foo.>", true},
		{"foo.>", "foo.bar", false},
		{"foo..bar", "foo.*", false}, // Bad subject, we return false
		{"foo.*", "foo..bar", false}, // Bad subject, we return false
	} {
		t.Run("", func(t *testing.T) {
			if res := SubjectIsSubsetMatch(test.subject, test.test); res != test.result {
				t.Fatalf("Subject %q subset match of %q, should be %v, got %v",
					test.test, test.subject, test.result, res)
			}
		})
	}
}
