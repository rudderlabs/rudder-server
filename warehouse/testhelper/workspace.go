package testhelper

import (
	"log"
	"os"
	"text/template"
)

func CreateWorkspaceConfig(path string, values map[string]string) string {
	t, err := template.ParseFiles(path)
	if err != nil {
		log.Panicf("Error occurred while parsing files for template path with error: %s", err.Error())
	}

	f, err := os.CreateTemp("", "workspaceConfig.*.json")
	if err != nil {
		log.Panicf("Error occurred while creating temp path with error: %s", err.Error())
	}
	defer func() { _ = f.Close() }()

	err = t.Execute(f, values)
	if err != nil {
		log.Panicf("Error occurred while executing template path files with error: %s", err.Error())
	}
	return f.Name()
}
