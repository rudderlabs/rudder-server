package types

//SingularEventT single event structrue
type SingularEventT map[string]interface{}

//GatewayBatchRequestT batch request structure
type GatewayBatchRequestT struct {
	Batch []SingularEventT `json:"batch"`
}
