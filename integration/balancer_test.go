package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	// TODO: Реалізуйте інтеграційний тест для балансувальникка.
	var previousLbFrom string

	// Send multiple requests
	for i := 0; i < 5; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			t.Error(err)
		}
		defer resp.Body.Close()

		lbFrom := resp.Header.Get("lb-from")
		if lbFrom == previousLbFrom {
			t.Errorf("lb-from has not changed. Previous: %s, Current: %s", previousLbFrom, lbFrom)
		}

		previousLbFrom = lbFrom
		t.Logf("response from [%s]", lbFrom)
		time.Sleep(time.Second)
	}

}

func BenchmarkBalancer(b *testing.B) {
	// TODO: Реалізуйте інтеграційний бенчмарк для балансувальникка.
}
