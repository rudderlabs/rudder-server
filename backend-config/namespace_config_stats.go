package backendconfig

import (
	"io"
	"maps"
	"net/http"
	"strconv"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

// requestDoer is the one method of http.Client the namespace config fetchers use.
type requestDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// fetchStatsDoer instruments every request a namespace config fetcher makes.
// Both stats are tagged with the fetcher's version and with mode=incremental when
// the request carries an updatedAfter query parameter, mode=full otherwise.
//
//   - backend_config_http_calls: one per attempt, retries included, tagged error=transport
//     for a failed round trip, error=http_<status> for a status of 300 and above and
//     error="" otherwise
//   - backend_config_http_response_size: bytes read off the response body, observed once
//     the caller closes it
type fetchStatsDoer struct {
	doer    requestDoer
	stats   stats.Stats
	version string
}

func (d *fetchStatsDoer) Do(req *http.Request) (*http.Response, error) {
	tags := stats.Tags{"version": d.version, "mode": "full"}
	if req.URL.Query().Has("updatedAfter") {
		tags["mode"] = "incremental"
	}
	resp, err := d.doer.Do(req)
	if err != nil {
		d.called(tags, "transport")
		return nil, err
	}
	if resp.StatusCode >= 300 {
		d.called(tags, "http_"+strconv.Itoa(resp.StatusCode))
	} else {
		d.called(tags, "")
	}
	// the size is known once the caller is done with the body: v2 decodes it as it streams
	resp.Body = &observedReadCloser{
		ReadCloser: resp.Body,
		observe:    d.stats.NewTaggedStat("backend_config_http_response_size", stats.HistogramType, tags).Observe,
	}
	return resp, nil
}

func (d *fetchStatsDoer) called(tags stats.Tags, errorReason string) {
	callTags := stats.Tags{"error": errorReason}
	maps.Copy(callTags, tags)
	d.stats.NewTaggedStat("backend_config_http_calls", stats.CountType, callTags).Increment()
}

// observedReadCloser reports how much was read out of it once it is closed.
type observedReadCloser struct {
	io.ReadCloser
	read    int
	observe func(float64)
}

func (r *observedReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.read += n
	return n, err
}

func (r *observedReadCloser) Close() error {
	r.observe(float64(r.read))
	return r.ReadCloser.Close()
}
