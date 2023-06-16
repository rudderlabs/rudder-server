package router_test

import (
	"encoding/json"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/router"
)

var _ = Describe("DestinationReponseHandler", func() {
	var (
		jsonHandler router.ResponseHandler
		body        string
		rules       map[string]interface{}
	)
	BeforeEach(func() {
		body = `{
		"message": { "id": "m1" },
      	"metadata": [{ "jobId": 1 }],
      	"success": false,
      	"errors": [{"code": 411}],
      	"destination": { "ID": "a", "url": "a" }
    	}`
		config := `{
  		"responseType": "JSON",
  		"rules": {
			"abortable": [{ "success": false, "errors.0.code": 411 }],
			"retryable": [{ "success": false, "errors.0.code": 403 },{ "success": false, "errors.0.code": 411 }]
  			}
		}`
		if err := json.Unmarshal([]byte(config), &rules); err != nil {
			fmt.Println(err)
		}
		jsonHandler = router.NewResponseHandler(logger.NOP, rules)
	})
	Context("Passing rules and body to be validates", func() {
		It("Non 200 codes are gives as is", func() {
			status := jsonHandler.IsSuccessStatus(403, body)
			Expect(status).To(Equal(403))
		})
		It("Non 200 codes are gives as is", func() {
			status := jsonHandler.IsSuccessStatus(500, body)
			Expect(status).To(Equal(500))
		})
		It("When body matches with abort config, 400 is returned", func() {
			status := jsonHandler.IsSuccessStatus(200, body)
			Expect(status).To(Equal(400))
		})
		It("When body matches with retryable config, 500 is returned", func() {
			body = `{
			"message": { "id": "m1" },
      		"metadata": [{ "jobId": 1 }],
      		"success": false,
      		"errors": [{"code": 403}],
      		"destination": { "ID": "a", "url": "a" }
    		}`
			status := jsonHandler.IsSuccessStatus(200, body)
			Expect(status).To(Equal(500))
		})
		It("must pass the responseCode as is when body doesnt match with config", func() {
			body = `{
			"message": { "id": "m1" },
      		"metadata": [{ "jobId": 1 }],
      		"success": false,
      		"errors": [{"code": 412}],
      		"destination": { "ID": "a", "url": "a" }
    		}`
			status := jsonHandler.IsSuccessStatus(200, body)
			Expect(status).To(Equal(200))
		})
		It("when responseType is not JSON/TXT, handler will be nil", func() {
			_ = body
			config := `{
  			"responseType": "csv",
  			"rules": {
			"abortable": [{ "success": false, "errors.0.code": 411 }],
			"retryable": [{ "success": false, "errors.0.code": 403 },{ "success": false, "errors.0.code": 411 }]
  			}
			}`
			if err := json.Unmarshal([]byte(config), &rules); err != nil {
				fmt.Println(err)
			}
			jsonHandler = router.NewResponseHandler(logger.NOP, rules)
			Expect(jsonHandler).To(BeNil())
		})
		It("when rules for a responseType are not present, handler will be nil", func() {
			_ = body
			var rules1 map[string]interface{}

			config := `{
  			"responseType": "JSON"
			}`
			if err := json.Unmarshal([]byte(config), &rules1); err != nil {
				fmt.Println(err)
			}
			jsonHandler = router.NewResponseHandler(logger.NOP, rules1)
			Expect(jsonHandler).To(BeNil())
		})
	})
})
