package trackingplan_validation

import "fmt"

type TPValidator struct{}

func (t *TPValidator) SendRequest(data interface{}) (interface{}, error) {
	fmt.Println("Sending request to Service A")
	// Add service-specific logic
	return "Response from Service A", nil
}
