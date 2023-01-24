package main

import (
	"log"
	"os"
	"path/filepath"
	"text/template"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
)

func main() {
	log.Println("Started populating workspace config")

	t, err := template.ParseFiles(testhelper.WorkspaceTemplatePath)
	if err != nil {
		log.Panicf("Error occurred while parsing files for template path with error: %s", err.Error())
	}

	err = os.MkdirAll(filepath.Dir(testhelper.WorkspaceConfigPath), os.ModePerm)
	if err != nil {
		log.Panicf("Error occurred while making directory paths error: %s", err.Error())
	}

	f, err := os.OpenFile(testhelper.WorkspaceConfigPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		log.Panicf("Error occurred while creating workspaceConfig.json file with error: %s", err.Error())
	}
	defer func() { _ = f.Close() }()

	templateConfigurations := testhelper.PopulateTemplateConfigurations()
	err = t.Execute(f, templateConfigurations)
	if err != nil {
		log.Panicf("Error occurred while executing template path files with error: %s", err.Error())
	}

	log.Println("Completed populating workspace config for warehouse integration tests")
}
