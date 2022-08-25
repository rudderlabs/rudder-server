package testhelper

import (
	"bytes"
	"html/template"
	"testing"

	"github.com/stretchr/testify/require"
)

func FillTemplateAndReturn(t *testing.T, templatePath string, values map[string]string) *bytes.Buffer {
	t.Helper()
	tpl, err := template.ParseFiles(templatePath)
	require.NoError(t, err)

	buf := bytes.NewBuffer(nil)
	require.NoError(t, tpl.Execute(buf, values))
	return buf
}
