package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"
)

var jsonObjects []map[string]interface{}

// List of Splunk HEC servers
var servers = []string{
	"https://server1.com:8088/services/collector/event",
	"https://server2.com:8088/services/collector/event",
	"https://server3.com:8088/services/collector/event",
	"https://server4.com:8088/services/collector/event",
	"https://server5.com:8088/services/collector/event",
}

func main() {
	// Start listening on port 8000
	log.Printf("Listening on :8000")
	http.HandleFunc("/", handler)
	http.ListenAndServe(":8000", nil)
}

func handler(w http.ResponseWriter, r *http.Request) {
	// Log the request
	log.Printf("%s %s %s\n", r.RemoteAddr, r.Method, r.URL)

	// Read the request body
	var data map[string]interface{}
	json.NewDecoder(r.Body).Decode(&data)

	// Add the index and sourcetype to the event
	event := map[string]interface{}{
		"event":      data,
		"index":      "main",
		"sourcetype": "generic_single_line",
	}

	// Add the event to the list of events
	jsonObjects = append(jsonObjects, event)

	// If we have 3 events, send them to Splunk HEC
	if len(jsonObjects) == 3 {
		randomizeServers()
		sent := false
		for _, server := range servers {
			sent = sendToSplunkHEC(w, jsonObjects, server)
			if sent {
				break
			}
		}
		if !sent {
			log.Println("Error: Failed to send data to all servers.")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("500 - Failed to send data to all servers."))
		}
		jsonObjects = nil
		return
	}

	// Return a 200 OK to the client
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func randomizeServers() {
	rand.Shuffle(len(servers), func(i, j int) {
		servers[i], servers[j] = servers[j], servers[i]
	})
}

func sendToSplunkHEC(w http.ResponseWriter, jsonObjects []map[string]interface{}, server string) bool {

	// Create a single string of all events
	var events []string
	for _, jsonObject := range jsonObjects {
		jsonBytes, _ := json.Marshal(jsonObject)
		events = append(events, string(jsonBytes))
	}
	eventsString := strings.Join(events, "")

	// Create a new request
	req, _ := http.NewRequest("POST", server, bytes.NewBuffer([]byte(eventsString)))
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Splunk x")
	req.Header.Add("Connection", "keep-alive")

	// Ignore TLS certificate errors
	tr := &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		DisableKeepAlives:   false,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     30 * time.Second,
	}

	// Send the request
	client := &http.Client{Transport: tr, Timeout: 10 * time.Second}
	resp, err := client.Do(req)

	// Handle the response
	if err != nil {
		if err, ok := err.(net.Error); ok && err.Timeout() {
			log.Printf("Timeout sending data to %s, retrying...\n", server)
			return false
		}
		log.Printf("Error sending data to %s: %s\n", server, err.Error())
		return false
	}
	defer resp.Body.Close()

	// Log the response
	if resp.StatusCode != 200 {
		log.Printf("Response from Splunk HEC: %d\n", resp.StatusCode)
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		log.Printf("Response Body: %s\n", string(bodyBytes))
	}
	log.Printf("Data sent to %s successfully\n", server)
	// Return the response to the client
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
	return true
}
