package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"sync"

	"cloud.google.com/go/firestore"
	firebase "firebase.google.com/go/v4"
	"google.golang.org/api/option"
)

var (
	client *firestore.Client
	mutex  sync.Mutex
)

type requestData struct {
	Field string  `json:"field"`
	Value float64 `json:"value"` // Use float64 to support numeric operations
}

type job struct {
	reqData requestData
	resp    http.ResponseWriter
}

func worker(ctx context.Context, jobs <-chan job) {
	for j := range jobs {
		processFirestoreUpdate(ctx, j.reqData, j.resp)
	}
}

func processFirestoreUpdate(ctx context.Context, reqData requestData, w http.ResponseWriter) {
	// Lock the critical section
	mutex.Lock()
	defer mutex.Unlock()

	docRef := client.Collection("test").Doc("9amFLocVwHFNEaskVVda")

	// Read the current document
	doc, err := docRef.Get(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting document: %v", err), http.StatusInternalServerError)
		return
	}

	// Prepare the new data by updating the existing value
	data := doc.Data()
	var currentValue float64

	// Check if the field exists and is a numeric value
	if existingValue, found := data[reqData.Field]; found {
		switch v := existingValue.(type) {
		case int64:
			currentValue = float64(v)
		case float64:
			currentValue = v //math.Round(v*100) / 100
		case string:
			if parsedValue, err := strconv.ParseFloat(v, 64); err == nil {
				currentValue = parsedValue
			} else {
				http.Error(w, "Current value is a non-numeric string", http.StatusBadRequest)
				return
			}
		default:
			http.Error(w, "Current value is not a number", http.StatusBadRequest)
			return
		}
	}

	// Add the new value to the current value
	newValue := currentValue + reqData.Value
	data[reqData.Field] = math.Trunc(newValue*1000) / 1000

	// Update the document in Firestore
	_, err = docRef.Set(ctx, data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error updating document: %v", err), http.StatusInternalServerError)
		return
	}

	// Respond to the client
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Successfully updated document. Field '%s' new value: %v", reqData.Field, newValue)
}

func addToFirestore(w http.ResponseWriter, r *http.Request, jobs chan<- job) {
	// Decode the request body
	var reqData requestData
	if err := json.NewDecoder(r.Body).Decode(&reqData); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Send the job to the worker pool
	jobs <- job{reqData: reqData, resp: w}
}

func main() {
	ctx := context.Background()

	// Initialize Firebase app with credentials
	opt := option.WithCredentialsFile("./key.json")
	app, err := firebase.NewApp(ctx, nil, opt)
	if err != nil {
		log.Fatalf("Error initializing app: %v", err)
	}

	// Get Firestore client
	client, err = app.Firestore(ctx)
	if err != nil {
		log.Fatalf("Error getting Firestore client: %v", err)
	}
	defer client.Close()

	// Create a job channel
	jobQueue := make(chan job, 1000)

	// Start a fixed number of workers
	numWorkers := 3
	for i := 0; i < numWorkers; i++ {
		go worker(ctx, jobQueue)
	}

	// Set up HTTP server
	http.HandleFunc("/add-to-firestore", func(w http.ResponseWriter, r *http.Request) {
		addToFirestore(w, r, jobQueue)
	})
	log.Println("Starting server on :8080...")
	if err := http.ListenAndServe("10.255.254.36:8080", nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
