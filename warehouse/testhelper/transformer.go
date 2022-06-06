package testhelper

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

func SetupTransformer() *TransformerResource {
	port := strconv.Itoa(54323)
	transformerEndPoint := fmt.Sprintf("http://localhost:%s", port)
	url := fmt.Sprintf("%s/health", transformerEndPoint)

	// Waiting until transformer is ready
	WaitUntilReady(context.Background(), url, time.Minute, time.Second, "transformer")

	return &TransformerResource{
		Url:  transformerEndPoint,
		Port: port,
	}
}
