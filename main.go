package main

import (
	"github.com/tzzh/pod-tzzh-kafka/babashka"
	"github.com/tzzh/pod-tzzh-kafka/kafka"
	"io/ioutil"
	"log"
	"os"
)

func main() {
	debug := os.Getenv("POD_TZZH_KAFKA_DEBUG")
	if debug != "true" {
		log.SetOutput(ioutil.Discard)
	}

	// global state to keep track of consumer/producer between calls
	state := make(map[string]interface{})

	for {
		message := babashka.ReadMessage()
		err := kafka.ProcessMessage(state, message)
		if err != nil {
			babashka.WriteErrorResponse(message, err)
		}
	}
}
