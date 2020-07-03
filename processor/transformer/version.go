package transformer

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

var cachedVersion string = ""

// Version returns the version returned by the configured transformer's /version endpoint.
// The version is requested once, and cached for any future calls to Version() function.
func Version() (version string, err error) {
	if cachedVersion == "" {
		v, err := getVersion()

		if err != nil {
			return "", err
		}

		cachedVersion = v
	}

	return cachedVersion, nil
}

func getVersion() (version string, err error) {
	tr := &http.Transport{}
	client := &http.Client{Transport: tr}

	response, err := client.Get(GetURL("/version"))
	if err != nil {
		panic(fmt.Errorf("Could not read transformer version: %w", err))
	}

	defer response.Body.Close()

	if response.StatusCode == http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(response.Body)
		if err != nil {
			panic(fmt.Errorf("Could not parse transformer version: %w", err))
		}

		bodyString := string(bodyBytes)
		return bodyString, nil
	}

	panic(fmt.Errorf("Could not read transformer version: Transformer status code was %v", response.StatusCode))
}
