package shared

type Metadata struct {
	WorkspaceID string

	SourceID           string
	SourceTaskRunID    string
	SourceJobID        string
	SourceJobRunID     string
	SourceDefinitionID string
	SourceCategory     string

	DestinationID           string
	DestinationDefinitionID string

	TransformationID        string
	TransformationVersionID string

	TrackingPlanID      string
	TrackingPlanVersion int

	EventType string
	EventName string
}

type InputEvent struct {
	Event     map[string]interface{}
	MessageID string
	Metadata  *Metadata
}

type OutputEvent struct {
	Event         map[string]interface{}
	MessageID     string
	Metadata      *Metadata
	StatusDetails *StatusDetails
}

type Mapping struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Incoming string `json:"incoming"`
	Target   string `json:"target"`
	Status   string `json:"status"`
	Count    int64  `json:"count"`
}

type StatusDetails struct {
	Status   string
	Mappings []Mapping
}
