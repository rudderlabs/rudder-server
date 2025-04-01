package transformer

import (
	"context"

	"github.com/rudderlabs/rudder-server/processor/types"
)

// SimpleMockDestinationClient is a minimal mock for DestinationClient
type SimpleMockDestinationClient struct {
	// Fixed responses to return
	TransformOutput types.Response
}

// Transform implements the DestinationClient interface
func (m *SimpleMockDestinationClient) Transform(_ context.Context, _ []types.TransformerEvent) types.Response {
	return m.TransformOutput
}

// SimpleMockUserClient is a minimal mock for UserClient
type SimpleMockUserClient struct {
	// Fixed response to return
	TransformOutput types.Response
}

// Transform implements the UserClient interface
func (m *SimpleMockUserClient) Transform(_ context.Context, _ []types.TransformerEvent) types.Response {
	return m.TransformOutput
}

// SimpleMockTrackingPlanClient is a minimal mock for TrackingPlanClient
type SimpleMockTrackingPlanClient struct {
	// Fixed response to return
	ValidateOutput types.Response
}

// Validate implements the TrackingPlanClient interface
func (m *SimpleMockTrackingPlanClient) Validate(_ context.Context, _ []types.TransformerEvent) types.Response {
	return m.ValidateOutput
}

// SimpleClients is a minimal implementation of TransformerClients
type SimpleClients struct {
	userClient         UserClient
	destinationClient  DestinationClient
	trackingPlanClient TrackingPlanClient
}

// NewSimpleClients creates a new instance of SimpleClients with empty responses
func NewSimpleClients() *SimpleClients {
	return &SimpleClients{
		userClient: &SimpleMockUserClient{
			TransformOutput: types.Response{
				Events:       []types.TransformerResponse{},
				FailedEvents: []types.TransformerResponse{},
			},
		},
		destinationClient: &SimpleMockDestinationClient{
			TransformOutput: types.Response{
				Events:       []types.TransformerResponse{},
				FailedEvents: []types.TransformerResponse{},
			},
		},
		trackingPlanClient: &SimpleMockTrackingPlanClient{
			ValidateOutput: types.Response{
				Events:       []types.TransformerResponse{},
				FailedEvents: []types.TransformerResponse{},
			},
		},
	}
}

// User returns the user client
func (s *SimpleClients) User() UserClient {
	return s.userClient
}

// Destination returns the destination client
func (s *SimpleClients) Destination() DestinationClient {
	return s.destinationClient
}

// TrackingPlan returns the tracking plan client
func (s *SimpleClients) TrackingPlan() TrackingPlanClient {
	return s.trackingPlanClient
}

// SetUserTransformOutput sets the response for the User transformer
func (s *SimpleClients) SetUserTransformOutput(response types.Response) {
	s.userClient = &SimpleMockUserClient{
		TransformOutput: response,
	}
}

// SetDestinationTransformOutput sets the response for the Destination transformer
func (s *SimpleClients) SetDestinationTransformOutput(response types.Response) {
	s.destinationClient = &SimpleMockDestinationClient{
		TransformOutput: response,
	}
}

// SetTrackingPlanValidateOutput sets the response for the TrackingPlan validator
func (s *SimpleClients) SetTrackingPlanValidateOutput(response types.Response) {
	s.trackingPlanClient = &SimpleMockTrackingPlanClient{
		ValidateOutput: response,
	}
}

// WithDynamicUserTransform sets a custom function for User transformer
func (s *SimpleClients) WithDynamicUserTransform(transformFn func(context.Context, []types.TransformerEvent) types.Response) {
	s.userClient = &dynamicUserClient{transformFn: transformFn}
}

// WithDynamicDestinationTransform sets a custom function for Destination transformer
func (s *SimpleClients) WithDynamicDestinationTransform(transformFn func(context.Context, []types.TransformerEvent) types.Response) {
	s.destinationClient = &dynamicDestinationClient{transformFn: transformFn}
}

// WithDynamicTrackingPlanValidate sets a custom function for TrackingPlan validator
func (s *SimpleClients) WithDynamicTrackingPlanValidate(validateFn func(context.Context, []types.TransformerEvent) types.Response) {
	s.trackingPlanClient = &dynamicTrackingPlanClient{validateFn: validateFn}
}

// Helper types for dynamic behavior

type dynamicUserClient struct {
	transformFn func(context.Context, []types.TransformerEvent) types.Response
}

func (d *dynamicUserClient) Transform(ctx context.Context, events []types.TransformerEvent) types.Response {
	return d.transformFn(ctx, events)
}

type dynamicDestinationClient struct {
	transformFn func(context.Context, []types.TransformerEvent) types.Response
}

func (d *dynamicDestinationClient) Transform(ctx context.Context, events []types.TransformerEvent) types.Response {
	return d.transformFn(ctx, events)
}

type dynamicTrackingPlanClient struct {
	validateFn func(context.Context, []types.TransformerEvent) types.Response
}

func (d *dynamicTrackingPlanClient) Validate(ctx context.Context, events []types.TransformerEvent) types.Response {
	return d.validateFn(ctx, events)
}

// Helper functions to create common responses

// EmptySuccessResponse creates an empty successful response
func EmptySuccessResponse() types.Response {
	return types.Response{
		Events:       []types.TransformerResponse{},
		FailedEvents: []types.TransformerResponse{},
	}
}

// SuccessResponse creates a response with successfully processed events
func SuccessResponse(outputs []map[string]interface{}, metadatas []types.Metadata) types.Response {
	responses := make([]types.TransformerResponse, len(outputs))
	for i, output := range outputs {
		metadata := types.Metadata{}
		if i < len(metadatas) {
			metadata = metadatas[i]
		}
		responses[i] = types.TransformerResponse{
			Output:     output,
			Metadata:   metadata,
			StatusCode: 200,
		}
	}
	return types.Response{
		Events:       responses,
		FailedEvents: []types.TransformerResponse{},
	}
}

// ErrorResponse creates a response with error information
func ErrorResponse(errorMsg string, statusCode int, metadata types.Metadata) types.Response {
	return types.Response{
		Events: []types.TransformerResponse{},
		FailedEvents: []types.TransformerResponse{
			{
				Output:     nil,
				Metadata:   metadata,
				StatusCode: statusCode,
				Error:      errorMsg,
			},
		},
	}
}
