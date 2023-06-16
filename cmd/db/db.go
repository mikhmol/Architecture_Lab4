package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/mikhmol/Architecture_Lab4/datastore"
	"github.com/mikhmol/Architecture_Lab4/httptools"
	"github.com/mikhmol/Architecture_Lab4/signal"
)

var port = flag.Int("port", 8080, "server port")

type Request struct {
	Value string `json:"value"`
}

func main() {
	log.Println("Intializing database server ...")

	r := mux.NewRouter()

	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		fmt.Println("Error creating temporary directory:", err)
		//os.Exit(1) // Exit with a non-zero error code
	}
	defer func() {
		os.RemoveAll(dir)
	}()

	db, err := datastore.NewDb(dir)
	if err != nil {
		fmt.Println("Error creating database:", err)
		os.Exit(1) // Exit with a non-zero error code
	}
	defer db.Close()

	r.HandleFunc("/db/{key}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		key := vars["key"]

		value, err := db.Get(key)
		if err != nil {
			if err == datastore.ErrNotFound {
				http.NotFound(w, r)
			} else {
				http.Error(w, err.Error(), http.StatusBadRequest)
			}
			return
		}

		response := struct {
			Key   string      `json:"key"`
			Value interface{} `json:"value"`
		}{
			Key:   key,
			Value: value,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}).Methods("GET")

	r.HandleFunc("/db/{key}", func(w http.ResponseWriter, r *http.Request) {
		var request Request
		vars := mux.Vars(r)
		key := vars["key"]

		err := json.NewDecoder(r.Body).Decode(&request)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = db.Put(key, request.Value)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(request)
	}).Methods("POST")

	server := httptools.CreateServer(*port, r)
	log.Println("Starting database server ...")
	server.Start()
	signal.WaitForTerminationSignal()
}
