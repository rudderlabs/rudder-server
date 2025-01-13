package destination_transformer

import "fmt"

type DestTransformer struct{}

func (d *DestTransformer) SendRequest(data interface{}) (interface{}, error) {
	fmt.Println("Sending request to Service A")
	// Add service-specific logic
	return "Response from Service A", nil
}
