package main

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

// copied from lib.go
func remote_delete(remote string) error {
	req, err := http.NewRequest("DELETE", remote, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		return fmt.Errorf("remote_delete: wrong status code %d", resp.StatusCode)
	}
	return nil
}

func remote_put(remote string, length int64, body io.Reader) error {
	req, err := http.NewRequest("PUT", remote, body)
	if err != nil {
		return err
	}
	req.ContentLength = length
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 && resp.StatusCode != 204 {
		return fmt.Errorf("remote_put: wrong status code %d", resp.StatusCode)
	}
	return nil
}

func remote_get(remote string) (string, error) {
	resp, err := http.Get(remote)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", errors.New(fmt.Sprintf("remote_get: wrong status code %d", resp.StatusCode))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	putReqs := make(chan struct{ key, value string }, 1000)
	getReqs := make(chan string, 1000)
	putResp := make(chan bool, 1000)
	getResp := make(chan bool, 1000)
	fmt.Println("starting thrasher")

	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100

	// Modify PUT goroutine
	for i := 0; i < 16; i++ {
		go func() {
			for req := range putReqs {
				if err := remote_put("http://localhost:3000/"+req.key, int64(len(req.value)), strings.NewReader(req.value)); err != nil {
					fmt.Println("PUT FAILED", err)
					putResp <- false
					continue
				}
				putResp <- true
			}
		}()
	}

	// Modify GET goroutine
	for i := 0; i < 16; i++ {
		go func() {
			for key := range getReqs {
				_, err := remote_get("http://localhost:3000/" + key)
				if err != nil {
					fmt.Println("GET FAILED", err)
					getResp <- false
					continue
				}
				getResp <- true
			}
		}()
	}

	putCount := 1000
	getTotalCount := 1_000_000
	getRounds := getTotalCount / putCount

	start := time.Now()
	keyValuePairs := make(map[string]string, putCount)

	// Perform all PUT operations first
	for i := 0; i < putCount; i++ {
		key := fmt.Sprintf("benchmark-%d", rand.Int())
		value := fmt.Sprintf("value-%d", rand.Int())
		keyValuePairs[key] = value
		putReqs <- struct{ key, value string }{key, value}
	}

	// Wait for all PUT operations to complete
	for i := 0; i < putCount; i++ {
		if <-putResp == false {
			fmt.Println("ERROR on PUT", i)
			os.Exit(-1)
		}
	}

	// Perform GET operations in multiple rounds
	for round := 0; round < getRounds; round++ {
		if round%100 == 0 {
			fmt.Printf("Starting GET round %d of %d\n", round+1, getRounds)
		}

		// Send all keys for GET requests
		for key := range keyValuePairs {
			getReqs <- key
		}

		// Wait for all GET operations in this round to complete
		for i := 0; i < putCount; i++ {
			if <-getResp == false {
				fmt.Printf("ERROR on GET, round %d, key %d\n", round+1, i)
				os.Exit(-1)
			}
		}
	}

	totalOps := putCount + getTotalCount
	duration := time.Since(start)
	fmt.Printf("Completed %d PUTs and %d GETs (%d rounds) in %v\n", putCount, getTotalCount, getRounds, duration)
	fmt.Printf("Total operations: %d, that's %.2f ops/sec\n", totalOps, float64(totalOps)/duration.Seconds())
}
