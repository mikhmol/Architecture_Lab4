package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/mikhmol/Architecture_Lab4/httptools"
	"github.com/mikhmol/Architecture_Lab4/signal"
)

var port = flag.Int("port", 8080, "server port")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"
const databaseURL = "http://database:8080/db/solo"

type Payload struct {
	Value string `json:"value"`
}

func main() {
	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		// Call database using http.DefaultClient
		resp, err := http.Get(databaseURL)
		if err != nil {
			http.Error(rw, "Error getting data", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			http.Error(rw, "Error reading data", http.StatusInternalServerError)
			return
		}

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		rw.Write(body)
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()

	// Init database
	jsonData := Payload{
		Value: "2023-06-16",
	}
	jsonValue, _ := json.Marshal(jsonData)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	client := http.Client{}
	req, err := http.NewRequestWithContext(ctx, "POST", databaseURL, bytes.NewBuffer(jsonValue))
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = client.Do(req)
	if err != nil {
		fmt.Println(err)
	}

	signal.WaitForTerminationSignal()
}
