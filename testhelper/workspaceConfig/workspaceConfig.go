package workspaceConfig

import (
	"os"
	"testing"
	"text/template"

	"github.com/stretchr/testify/require"
)

func CreateTempFile(t testing.TB, templatePath string, values map[string]any) string {
	t.Helper()
	tpl, err := template.ParseFiles(templatePath)
	require.NoError(t, err)

	tmpFile, err := os.CreateTemp("", "workspaceConfig.*.json")
	require.NoError(t, err)
	defer func() { _ = tmpFile.Close() }()

	require.NoError(t, tpl.Execute(tmpFile, values))
	t.Cleanup(func() {
		if err := os.Remove(tmpFile.Name()); err != nil {
			t.Logf("Error while removing workspace config: %v", err)
		}
	})

	return tmpFile.Name()
}
