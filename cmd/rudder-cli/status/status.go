package status

import (
	"fmt"
	"net/http"
	"os"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli/v2"

	"github.com/rudderlabs/rudder-go-kit/httputil"

	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/client"
	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/config"
	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/util"
)

func goBackendStatus() (string, error) {
	backendURL := "http://localhost:8080"
	resp, err := http.Get(fmt.Sprintf("%s/health", backendURL))
	if err != nil {
		fmt.Println(err)
		return "failed", err
	}
	defer httputil.CloseResponse(resp)

	return "success", nil
}

func transformerStatus() (string, error) {
	transformerURL := config.GetEnv(config.DestTransformURLKey)
	resp, err := http.Get(fmt.Sprintf("%s/health", transformerURL))
	if err != nil {
		return "failed", err
	}
	defer httputil.CloseResponse(resp)

	return "success", nil
}

func dbStatus() (string, error) {
	return util.IsDBConnected()
}

func checkDumpDestination() (string, error) {
	return util.TestUpload()
}

func Flags() []cli.Flag {
	return []cli.Flag{
		// cli.StringFlag{
		// 	Name:   "url",
		// 	Usage:  "Base URL where for the data plane Eg. http://localhost:8080",
		// 	EnvVar: "RUDDER_BASE_URL",
		// },
	}
}

func beautifyMessage(msg string, err error) string {
	if err != nil {
		return util.RedColor(msg)
	}
	return util.GreenColor(msg)
}

func DisplayStatus() {
	table := tablewriter.NewWriter(os.Stdout)

	table.SetAutoWrapText(false)
	table.SetHeader([]string{"Service", "Status"})

	message, err := goBackendStatus()
	table.Append([]string{
		"Backend",
		beautifyMessage(message, err),
	})

	message, err = transformerStatus()
	table.Append([]string{
		"Transformer",
		beautifyMessage(message, err),
	})

	message, err = dbStatus()
	table.Append([]string{
		"Database",
		beautifyMessage(message, err),
	})

	message, err = checkDumpDestination()
	table.Append([]string{
		"Table Dump Permissions",
		beautifyMessage(message, err),
	})

	table.Render()
}

func GetArgs(request string) error {
	if request == "Jobs between JobID's of a User" {
		fmt.Printf("Enter DS Type:")
		var table string
		_, _ = fmt.Scanf("%s", &table)
		fmt.Print("Enter JobID 1: ")
		var input string
		_, _ = fmt.Scanf("%s", &input)
		fmt.Print("Enter JobID 2: ")
		var input2 string
		_, _ = fmt.Scanf("%s", &input2)
		fmt.Printf("Enter UserID:")
		var input3 string
		_, _ = fmt.Scanf("%s", &input3)
		argString := table + ":" + request + ":" + input + ":" + input2 + ":" + input3
		var reply string
		err := client.GetUDSClient().Call("JobsdbUtilsHandler.RunSQLQuery", argString, &reply)
		if err == nil {
			fmt.Println(reply)
		}
		return err
	} else if request == "Error Code Count By Destination" {
		fmt.Printf("Enter DS Type:")
		var table string
		_, _ = fmt.Scanf("%s", &table)
		fmt.Print("Enter DS Number: ")
		var dsNum string
		_, _ = fmt.Scanf("%s", &dsNum)
		argString := table + ":" + request + ":" + dsNum
		var reply string
		err := client.GetUDSClient().Call("JobsdbUtilsHandler.RunSQLQuery", argString, &reply)
		if err == nil {
			fmt.Println(reply)
		}
		return err
	}
	return nil
}
