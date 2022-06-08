package testhelper

import (
	"fmt"
	"strconv"
)

func SetupTransformer() *TransformerResource {
	port := strconv.Itoa(54323)
	transformerEndPoint := fmt.Sprintf("http://localhost:%s", port)

	return &TransformerResource{
		Url:  transformerEndPoint,
		Port: port,
	}
}
