package user_transformer

import "fmt"

type UserTransformer struct{}

func (u *UserTransformer) SendRequest(data interface{}) (interface{}, error) {
	fmt.Println("Sending request to Service A")
	// Add service-specific logic
	return "Response from Service A", nil
}
