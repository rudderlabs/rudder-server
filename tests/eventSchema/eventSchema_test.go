package eventSchema_test

import (
	"database/sql"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/tests/helpers"
)

var dbHandle *sql.DB
var gatewayDBPrefix string
var routerDBPrefix string
var dbPollFreqInS int = 1
var gatewayDBCheckBufferInS int = 5
var jobSuccessStatus string = "succeeded"


var _ = BeforeSuite(func() {
	var err error
	psqlInfo := helpers.GetConnectionString()
	dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	gatewayDBPrefix = config.GetString("Gateway.CustomVal", "GW")
	routerDBPrefix = config.GetString("Router.CustomVal", "RT")
})

var _ = Describe("EventSchema", func() {

	Context("Without user sessions processing", func() {

		It("verify event schema is stored in config backend db", func() {
			initEventSchemaCount := helpers.FetchEventSchemaCount(dbHandle)

			helpers.SendEventRequest(helpers.EventOptsT{})
		
			Eventually(func() int {
				return helpers.FetchEventSchemaCount(dbHandle)
			}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(initEventSchemaCount + 1))
		
		})
	})
})
