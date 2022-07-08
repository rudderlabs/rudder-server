package suppression

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

var _ = Describe("SuppressUser Test", func() {
	testSuppressUser := new(SuppressRegulationHandler)
	BeforeEach(func() {
		config.Load()
		logger.Init()
		backendconfig.Init()
		pkgLogger = logger.NewLogger().Child("enterprise").Child("suppress-user")
		testSuppressUser = &SuppressRegulationHandler{Client: new(http.Client), RegulationsPollInterval: time.Duration(100)}
	})
	expectedRespRegulations := sourceRegulation{
		Canceled:  false,
		UserID:    "user-1",
		SourceIDs: []string{"src-1", "src-2"},
	}
	expectedResp := apiResponse{
		SourceRegulations: []sourceRegulation{expectedRespRegulations},
		Token:             "tempToken123",
	}

	Context("getSourceRegulationsFromRegulationService error cases", func() {
		It("wrong server address", func() {
			srv := createSimpleTestServer(nil)
			defer srv.Close()
			_, err := testSuppressUser.getSourceRegulationsFromRegulationService()
			Expect(err.Error()).NotTo(Equal(nil))
		})

		It("500 server error", func() {
			srv := createSimpleTestServer(&serverInp{statusCode: 500})
			defer srv.Close()
			testSuppressUser.RegulationBackendURL = srv.URL
			_, err := testSuppressUser.getSourceRegulationsFromRegulationService()
			Expect(err.Error()).To(Equal("status code 500"))
		})

		It("invalid data in response body", func() {
			srv := createSimpleTestServer(&serverInp{statusCode: 200, respBody: []byte("")})
			defer srv.Close()
			testSuppressUser.RegulationBackendURL = srv.URL
			_, err := testSuppressUser.getSourceRegulationsFromRegulationService()
			Expect(err.Error()).To(Equal("unexpected end of JSON input"))
		})

		It("invalid data in response body", func() {
			srv := createSimpleTestServer(&serverInp{statusCode: 200, respBody: []byte("{w")})
			defer srv.Close()
			testSuppressUser.RegulationBackendURL = srv.URL
			_, err := testSuppressUser.getSourceRegulationsFromRegulationService()
			Expect(err.Error()).To(Equal("invalid character 'w' looking for beginning of object key string"))
		})

		It("no token in response body", func() {
			srv := createSimpleTestServer(&serverInp{statusCode: 200, respBody: []byte("{}")})
			defer srv.Close()
			testSuppressUser.RegulationBackendURL = srv.URL
			_, err := testSuppressUser.getSourceRegulationsFromRegulationService()
			Expect(err.Error()).To(Equal("no token returned in regulation API response"))
		})
	})

	Context("getSourceRegulationsFromRegulationService valid response", func() {
		It("no token in response body", func() {
			tempResp := expectedResp
			tempResp.Token = ""
			expectedRespBody, _ := json.Marshal(tempResp)
			srv := createSimpleTestServer(&serverInp{statusCode: 200, respBody: expectedRespBody})
			defer srv.Close()
			testSuppressUser.RegulationBackendURL = srv.URL
			resp, err := testSuppressUser.getSourceRegulationsFromRegulationService()
			Expect(err.Error()).To(Equal("no token returned in regulation API response"))
			Expect(resp).To(Equal([]sourceRegulation{expectedRespRegulations}))
		})
	})

	Context("IsSuppressedUser", func() {
		It("user suppression rule added and user-id is same", func() {
			r := expectedRespRegulations
			expectedResp := apiResponse{
				SourceRegulations: []sourceRegulation{r},
				Token:             "tempToken123",
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				queryParams := r.URL.Query()["pageToken"]
				if len(queryParams) != 0 {
					expectedResp = apiResponse{
						Token: "tempToken123",
					}
					expectedRespBody, _ := json.Marshal(expectedResp)
					w.Write(expectedRespBody)
				} else {
					expectedRespBody, _ := json.Marshal(expectedResp)
					w.Write(expectedRespBody)
				}
			}))
			defer srv.Close()
			testSuppressUser.RegulationBackendURL = srv.URL
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			testSuppressUser.setup(ctx)
			Eventually(func() bool { return testSuppressUser.IsSuppressedUser("user-1", "src-1", "") }).Should(BeTrue())
		})

		It("user suppression cancelled after adding first", func() {
			tempResp := expectedResp
			tempResp.SourceRegulations = append(tempResp.SourceRegulations, sourceRegulation{
				Canceled:  false,
				UserID:    "user-2",
				SourceIDs: []string{"src-1", "src-2"},
			})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				queryParams := r.URL.Query()["pageToken"]
				if len(queryParams) != 0 {
					r := expectedRespRegulations
					r.Canceled = true
					expectedResp := apiResponse{
						SourceRegulations: []sourceRegulation{r},
						Token:             "tempToken123",
					}
					expectedRespBody, _ := json.Marshal(expectedResp)
					w.Write(expectedRespBody)
				} else {
					expectedRespBody, _ := json.Marshal(tempResp)
					w.Write(expectedRespBody)
				}
			}))
			defer srv.Close()
			// testSuppressUser := &SuppressRegulationHandler{Client: new(http.Client), RegulationsPollInterval: time.Duration(100),
			//	RegulationBackendURL: srv.}
			testSuppressUser.RegulationBackendURL = srv.URL
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			testSuppressUser.setup(ctx)
			Eventually(func() bool { return testSuppressUser.IsSuppressedUser("user-2", "src-1", "") }).Should(BeTrue())
			Eventually(func() bool { return testSuppressUser.IsSuppressedUser("user-1", "src-1", "") }).Should(BeFalse())
		})

		It("user suppression rule added for all the sources", func() {
			r1 := sourceRegulation{
				Canceled:  false,
				UserID:    "user-1",
				SourceIDs: []string{},
			}
			r2 := sourceRegulation{
				Canceled:  false,
				UserID:    "user-2",
				SourceIDs: []string{"src-2"},
			}
			expectedResp := apiResponse{
				SourceRegulations: []sourceRegulation{r1, r2},
				Token:             "tempToken123",
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				queryParams := r.URL.Query()["pageToken"]
				if len(queryParams) != 0 {
					expectedResp = apiResponse{
						Token: "tempToken123",
					}
					expectedRespBody, _ := json.Marshal(expectedResp)
					w.Write(expectedRespBody)
				} else {
					expectedRespBody, _ := json.Marshal(expectedResp)
					w.Write(expectedRespBody)
				}
			}))
			defer srv.Close()
			testSuppressUser.RegulationBackendURL = srv.URL
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			testSuppressUser.setup(ctx)
			Eventually(func() bool { return testSuppressUser.IsSuppressedUser("user-1", "src-1", "") }).Should(BeTrue())
			Eventually(func() bool { return testSuppressUser.IsSuppressedUser("user-2", "src-2", "") }).Should(BeTrue())
			Eventually(func() bool { return testSuppressUser.IsSuppressedUser("user-2", "src-1", "") }).Should(BeFalse())
		})

		It("user suppression rule added for all the sources and then cancelled", func() {
			r1 := sourceRegulation{
				Canceled:  false,
				UserID:    "user-1",
				SourceIDs: []string{},
			}
			r2 := sourceRegulation{
				Canceled:  false,
				UserID:    "user-2",
				SourceIDs: []string{"src-2"},
			}
			expectedResp := apiResponse{
				SourceRegulations: []sourceRegulation{r1, r2},
				Token:             "tempToken123",
			}
			firstCheck := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				queryParams := r.URL.Query()["pageToken"]
				if len(queryParams) != 0 {
					<-firstCheck
					r1 = sourceRegulation{
						Canceled:  true,
						UserID:    "user-1",
						SourceIDs: []string{},
					}
					expectedResp = apiResponse{
						SourceRegulations: []sourceRegulation{r1},
						Token:             "tempToken123",
					}
					expectedRespBody, _ := json.Marshal(expectedResp)
					w.Write(expectedRespBody)
				} else {
					expectedRespBody, _ := json.Marshal(expectedResp)
					w.Write(expectedRespBody)
				}
			}))
			defer srv.Close()
			testSuppressUser.RegulationBackendURL = srv.URL
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			testSuppressUser.setup(ctx)
			Eventually(func() bool { return testSuppressUser.IsSuppressedUser("user-1", "src-1", "") }).Should(BeTrue())
			Eventually(func() bool { return testSuppressUser.IsSuppressedUser("user-2", "src-2", "") }).Should(BeTrue())
			Eventually(func() bool { return testSuppressUser.IsSuppressedUser("user-2", "src-1", "") }).Should(BeFalse())
			close(firstCheck)
			Eventually(func() bool { return testSuppressUser.IsSuppressedUser("user-1", "src-1", "") }).Should(BeFalse())
		})
	})
})

type serverInp struct {
	statusCode int
	respBody   []byte
}

func createSimpleTestServer(inp *serverInp) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if inp != nil {
			w.WriteHeader(inp.statusCode)
			_, err := w.Write(inp.respBody)
			if err != nil {
				fmt.Println("failed to write data to response body in test server")
				return
			}
		}
	}))
}

func TestSuppressRegulationHandler_IsSuppressedUser(t *testing.T) {
	config.Load()
	logger.Init()
	pkgLogger = logger.NewLogger().Child("enterprise").Child("suppress-user")

	suppressUserMap := make(map[string]sourceFilter)
	suppressUserMap["user1"] = sourceFilter{
		all:      true,
		specific: nil,
	}
	specificSrc := map[string]struct{}{
		"src1": {},
		"src2": {},
	}
	suppressUserMap["user2"] = sourceFilter{
		all:      false,
		specific: specificSrc,
	}
	s := &SuppressRegulationHandler{
		userSpecificSuppressedSourceMap: suppressUserMap,
	}

	require.True(t, s.IsSuppressedUser("user1", "src1", ""))
	require.True(t, s.IsSuppressedUser("user1", "randomNewSrc", ""))
	require.True(t, s.IsSuppressedUser("user2", "src1", ""))
	require.True(t, s.IsSuppressedUser("user2", "src2", ""))
	require.False(t, s.IsSuppressedUser("user2", "src3", ""))
	require.False(t, s.IsSuppressedUser("user2", "randomNewSrc", ""))
}
