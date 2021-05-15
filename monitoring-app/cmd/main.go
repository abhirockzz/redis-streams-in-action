package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	client                 *redis.Client
	streamName             string
	consumerGroupName      string
	monitoringConsumerName string
	minIdleTimeSec         int
)

const (
	redisHostEnv                   = "REDIS_HOST"
	redisPasswordEnv               = "REDIS_PASSWORD"
	streamNameEnv                  = "STREAM_NAME"
	consumerGroupNameEnv           = "STREAM_CONSUMER_GROUP_NAME"
	indexDefinitionHashPrefix      = "tweet:"
	monitoringConsumerGroupNameEnv = "MONITORING_CONSUMER_NAME"
	minIdleTimeEnv                 = "MIN_IDLE_TIME_SEC"
)

func init() {
	host := GetEnvOrFail(redisHostEnv)
	password := GetEnvOrFail(redisPasswordEnv)
	streamName = GetEnvOrFail(streamNameEnv)
	consumerGroupName = GetEnvOrFail(consumerGroupNameEnv)
	monitoringConsumerName = GetEnvOrFail(monitoringConsumerGroupNameEnv)

	minIdleTimeStr := GetEnvOrFail(minIdleTimeEnv)

	var err error
	minIdleTimeSec, err = strconv.Atoi(minIdleTimeStr)
	if err != nil {
		log.Fatal(err)
	}

	//log.Println(fmt.Sprintf("metadata: %s %s %s %s %s %d\n", host, password, streamName, consumerGroupName, monitoringConsumerName, minIdleTimeSec))

	client = redis.NewClient(&redis.Options{Addr: host, Password: password, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}})

	err = client.Ping(context.Background()).Err()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("connected to Redis")

}

func main() {
	defer func() {
		err := client.Close()
		if err == nil {
			log.Println("closed redis connection")
		}
	}()

	port, exists := os.LookupEnv("FUNCTIONS_CUSTOMHANDLER_PORT")
	if !exists {
		port = "8080"
	}
	http.HandleFunc("/monitor", process)
	log.Println("Go server listening on port", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))

}

func process(rw http.ResponseWriter, req *http.Request) {
	/*resp, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Fatal(err)
	}
	defer req.Body.Close()
	log.Println("request body", string(resp))*/

	// we call XPENDING to know no. of pending messages e.g. XPENDING tweets_stream group1
	numPendingMessages := client.XPending(context.Background(), streamName, consumerGroupName).Val().Count
	log.Println("No. of pending messages =", numPendingMessages)

	if numPendingMessages == 0 {
		rw.Header().Add("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(NoPendingMessagesResponse)
		return
	}

	// we get more details of the pending messages e.g. XPENDING tweets_stream group1 - + <count (from above)>
	xpendingResult := client.XPendingExt(context.Background(), &redis.XPendingExtArgs{Stream: streamName, Group: consumerGroupName, Start: "-", End: "+", Count: numPendingMessages})

	err := xpendingResult.Err()
	if err != nil {
		//log.Fatal(err)
		json.NewEncoder(rw).Encode(err.Error())
		return
	}

	pendingMessages := xpendingResult.Val()

	var toBeClaimed []string
	for _, msg := range pendingMessages {
		// create a slice for the XCLAIM execution (next step)
		toBeClaimed = append(toBeClaimed, msg.ID)
	}

	// claim messages: XCLAIM tweets_stream group1 monitoring-consumer <duration> <IDs - slice obtained by XPENDING>

	//only messages that are older than min-idle-time will be claimed. others will remain in pending state and will be returned by next call to XPENDING

	xclaim := client.XClaim(context.Background(), &redis.XClaimArgs{Stream: streamName, Group: consumerGroupName, Consumer: monitoringConsumerName, MinIdle: time.Duration(minIdleTimeSec) * time.Second, Messages: toBeClaimed})

	err = xclaim.Err()
	if err != nil {
		//log.Fatal(err)
		json.NewEncoder(rw).Encode(err.Error())
		return
	}

	if len(xclaim.Val()) == 0 {
		log.Println("No messages pending for more than", minIdleTimeSec, "sec")
		NoClaimedMessagesResponse.Pending = len(pendingMessages)

		rw.Header().Add("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(NoClaimedMessagesResponse)
		return
	}
	log.Println(fmt.Sprintf("No. of messages claimed %d (these have been not been processed since %d sec)", len(xclaim.Val()), minIdleTimeSec))

	var waitGroup sync.WaitGroup
	success := 0
	start := time.Now()
	for _, claimed := range xclaim.Val() {

		waitGroup.Add(1)

		// goroutines are pretty in-expensive. for each tweet, a goroutine will be spawned since this is a quick operation - HSET and XACK

		go func(tweetFromStream redis.XMessage) {

			hashName := fmt.Sprintf("%s%s", indexDefinitionHashPrefix, tweetFromStream.Values["id"])

			processed := false
			defer func() {
				waitGroup.Done()
				if processed {
					//log.Println("added tweet info to redis -", hashName)
					success++
				}
			}()

			err = client.HSet(context.Background(), hashName, tweetFromStream.Values).Err()

			if err != nil {
				log.Println("failed to add tweet info", hashName, "due to", err)
				return // don't proceed (ACK) if HSET fails
			}

			err = client.XAck(context.Background(), streamName, consumerGroupName, tweetFromStream.ID).Err()
			if err != nil {
				log.Println("failed to ACK tweet info", hashName, "due to", err)
				return
			}
			processed = true
		}(claimed)
	}
	log.Println("waiting for batch to get processed")
	waitGroup.Wait()
	processingTime := time.Since(start).Seconds()
	log.Println("finished processing batch of", len(xclaim.Val()), "messages")

	rw.Header().Add("Content-Type", "application/json")
	json.NewEncoder(rw).Encode(ProcessResult{Pending: len(pendingMessages), Claimed: len(xclaim.Val()), Processed: success, TimeTakenSecs: processingTime})
}

// GetEnvOrFail fetches value for the given env var or fails with a message
func GetEnvOrFail(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("Environment variable %s not set", key)
	}

	return val
}

type ProcessResult struct {
	Pending       int
	Claimed       int
	Processed     int
	TimeTakenSecs float64 `json:",omitempty"`
}

type ErrorResponse struct {
	ErrorMsg string
}

var NoPendingMessagesResponse = ProcessResult{Pending: 0, Claimed: 0, Processed: 0}
var NoClaimedMessagesResponse = ProcessResult{Claimed: 0, Processed: 0}
