package types

import (
	"slices"

	"github.com/rudderlabs/rudder-server/gateway/response"
)

// StatReason is the reason a request did not succeed, as reported on the per-source stats.
//
// It is a closed set. Every value below becomes a Prometheus label, so an open one - an error message, say, which may
// be formatted - would be unbounded cardinality. The interface is sealed by the unexported protected method: a caller
// outside this package can hold a StatReason and read its Value, but cannot bring a new one into existence. That is
// what lets a consumer of StatReporter switch over the set exhaustively instead of matching strings, and what makes a
// reason added here a compile-visible gap in that switch rather than a silent fallthrough.
//
// Where no bounded reason fits, report the closest one and log the underlying error: the text belongs in the logs,
// where it stays searchable, not in a label.
type StatReason interface {
	protected()
	Value() string
}

type statReason struct{ v string }

func (r *statReason) protected()    {}
func (r *statReason) Value() string { return r.v }

// Reasons returns every declared reason. A consumer outside this package uses it to check, in a test, that it still
// handles the whole set - so a reason added here surfaces as a failure there rather than as an unmapped value in
// production. The slice is a copy; the reasons themselves are immutable.
func Reasons() []StatReason {
	return slices.Clone(allReasons)
}

var allReasons []StatReason

// StatReasonForMessage resolves one of this gateway's own error messages to the reason declared for it. Several
// internal errors are built as errors.New(response.X), so the message is already the bounded identifier - this maps it
// back to the declared value rather than letting the message reach a label directly. It reports false for anything
// else, which the caller should report under a catch-all reason and log.
func StatReasonForMessage(v string) (StatReason, bool) {
	r, ok := reasonsByValue[v]
	return r, ok
}

var reasonsByValue = map[string]StatReason{}

// newReason declares a reason and registers it. Every reason below is built with it, which is what makes Reasons()
// complete without a second list to keep in step.
func newReason(v string) StatReason {
	r := &statReason{v: v}
	allReasons = append(allReasons, r)
	reasonsByValue[v] = r
	return r
}

// Reasons a request was dropped rather than failed: it was turned away deliberately, not broken.
var (
	// ReasonRateLimit marks a request refused because the source is over its limit. It is the reason an operator
	// alerts on to find a source losing data continuously, so report it only where a rate limit is what happened.
	ReasonRateLimit StatReason = newReason("rate limit")
)

// Reasons a request failed. The values mirror the strings these sites reported before StatReason existed, so the
// series they produce are unchanged.
var (
	ReasonNoWriteKeyInBasicAuth   StatReason = newReason(response.NoWriteKeyInBasicAuth)
	ReasonNoWriteKeyInQueryParams StatReason = newReason(response.NoWriteKeyInQueryParams)
	ReasonInvalidWriteKey         StatReason = newReason(response.InvalidWriteKey)
	ReasonInvalidSourceID         StatReason = newReason(response.InvalidSourceID)
	ReasonNoSourceIDInHeader      StatReason = newReason(response.NoSourceIdInHeader)
	ReasonSourceDisabled          StatReason = newReason(response.SourceDisabled)
	ReasonNoDestinationIDInHeader StatReason = newReason(response.NoDestinationIDInHeader)
	ReasonInvalidDestinationID    StatReason = newReason(response.InvalidDestinationID)
	ReasonDestinationDisabled     StatReason = newReason(response.DestinationDisabled)

	// ReasonAuthenticatingWebhookRequest marks a webhook request whose write key could not be looked up at all, as
	// opposed to one looked up and found invalid.
	ReasonAuthenticatingWebhookRequest StatReason = newReason(response.ErrAuthenticatingWebhookRequest)
	ReasonInvalidReplaySource          StatReason = newReason(response.InvalidReplaySource)
	// ReasonInvalidRequestContext marks a request that reached a handler without the context a middleware should
	// have put on it. It is a bug in the middleware chain, not something the caller did.
	ReasonInvalidRequestContext StatReason = newReason("invalid request context")

	// ReasonWriteKeyNotFound and ReasonWriteKeyMissingFromQuery report the same conditions as ReasonInvalidWriteKey
	// and ReasonNoWriteKeyInQueryParams, but from the sites that tagged them with these shorter strings instead of
	// the wire message. Kept distinct so the existing series keep their values; worth collapsing into one reason
	// each, in a change that is allowed to move them.
	ReasonWriteKeyNotFound         StatReason = newReason("invalidWriteKey")
	ReasonWriteKeyMissingFromQuery StatReason = newReason("NoWriteKeyInQueryParams")

	ReasonEmptyBatchPayload      StatReason = newReason(response.EmptyBatchPayload)
	ReasonNonIdentifiableRequest StatReason = newReason(response.NonIdentifiableRequest)
	ReasonInvalidRequestMethod   StatReason = newReason(response.InvalidRequestMethod)
	ReasonRequestBodyNil         StatReason = newReason(response.RequestBodyNil)
	ReasonInvalidJSON            StatReason = newReason(response.InvalidJSON)
	ReasonNotRudderEvent         StatReason = newReason(response.NotRudderEvent)
	ReasonRequestBodyTooLarge    StatReason = newReason(response.RequestBodyTooLarge)
	ReasonRequestBodyReadFailed  StatReason = newReason("requestBodyReadFailed")
	ReasonStoreFailed            StatReason = newReason("storeFailed")

	ReasonCouldNotParseForm      StatReason = newReason("couldNotParseForm")
	ReasonCouldNotParseMultiform StatReason = newReason("couldNotParseMultiform")
	ReasonCouldNotMarshal        StatReason = newReason("couldNotMarshal")

	ReasonSourceTransformerNonSuccessResponse StatReason = newReason(response.SourceTransformerNonSuccessResponse)
	ReasonSourceTransformerResponseError      StatReason = newReason(response.SourceTransformerResponseError)
)

// Reasons introduced with StatReason, at sites that reported a raw error message before. The message itself is logged
// at each of them, so nothing is lost: it moves from a label, where it was unbounded, to a log line, where it is not.
var (
	// ReasonRequestProcessingFailed covers the errors building a job from a request that have no reason of their own.
	ReasonRequestProcessingFailed StatReason = newReason("request processing failed")
	// ReasonValidationFailed covers a message the validators rejected. Their messages come from rudder-schemas and are
	// not ours to bound, so the reason is this and the message goes to the log.
	ReasonValidationFailed StatReason = newReason("validation failed")
	// ReasonMarshalEventBatchFailed marks a single event batch that could not be marshalled for storage.
	ReasonMarshalEventBatchFailed StatReason = newReason("marshalling event batch failed")
)
