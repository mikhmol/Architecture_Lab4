package integration

import (
	"encoding/json"
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

type Payload struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func TestDatabaseServer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
	if err != nil {
		t.Error(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Unexpected status code: %d", resp.StatusCode)
		return
	}

	var data Payload
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		t.Error(err)
		return
	}

	expectedData := Payload{
		Key:   "solo",
		Value: "2023-06-16",
	}

	if data != expectedData {
		t.Errorf("Unexpected JSON response. Got %+v, want %+v", data, expectedData)
	}

}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

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
	}

}

func BenchmarkBalancer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			b.Error(err)
		}
		resp.Body.Close()
	}
}
