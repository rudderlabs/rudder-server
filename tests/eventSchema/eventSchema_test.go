package eventSchema_test

import (
	"database/sql"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/tests/helpers"
)

var dbHandle *sql.DB
var dbPollFreqInS int = 1
var gatewayDBCheckBufferInS int = 5

var _ = BeforeSuite(func() {
	var err error
	psqlInfo := helpers.GetConnectionString()
	dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
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
