package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tzzh/pod-tzzh-kafka/babashka"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"time"
)

func checkArgs(fnName string, rawArgs []json.RawMessage, argsNb int) error {
	if len(rawArgs) != argsNb {
		errorMsg := fmt.Sprintf("Wrong number of argument, %s expects %d arguments", fnName, argsNb)
		return errors.New(errorMsg)
	}
	return nil
}

func getAdminClient(state map[string]interface{}, adminClientName string) (*kafka.AdminClient, error) {
	rawAdminClient, ok := state[adminClientName]
	if !ok {
		return nil, errors.New("This client doesn't exist")
	}
	adminClient, ok := rawAdminClient.(*kafka.AdminClient)
	if !ok {
		return nil, errors.New("This client is not an AdminClient")
	}
	return adminClient, nil
}

func getProducer(state map[string]interface{}, producerName string) (*kafka.Producer, error) {
	rawProducer, ok := state[producerName]
	if !ok {
		return nil, errors.New("This client doesn't exist")
	}
	producer, ok := rawProducer.(*kafka.Producer)
	if !ok {
		return nil, errors.New("This client is not a Producer")
	}
	return producer, nil
}

func getConsumer(state map[string]interface{}, consumerName string) (*kafka.Consumer, error) {
	rawConsumer, ok := state[consumerName]
	if !ok {
		return nil, errors.New("This client doesn't exist")
	}
	consumer, ok := rawConsumer.(*kafka.Consumer)
	if !ok {
		return nil, errors.New("This client is not a Consumer")
	}
	return consumer, nil
}

func ProcessMessage(state map[string]interface{}, message *babashka.Message) error {
	if message.Op == "describe" {
		response := &babashka.DescribeResponse{
			Format: "json",
			Namespaces: []babashka.Namespace{
				{Name: "pod.tzzh.kafka",
					Vars: []babashka.Var{
						{Name: "new-admin-client"},
						{Name: "get-metadata"},
						{Name: "create-topics"},
						{Name: "delete-topics"},
						{Name: "new-producer"},
						{Name: "close-producer"},
						{Name: "produce"},
						{Name: "new-consumer"},
						{Name: "close-consumer"},
						{Name: "subscribe-topics"},
						{Name: "read-message"},
					},
				},
			},
		}
		babashka.WriteDescribeResponse(response)

	} else if message.Op == "invoke" {

		rawArgs := []json.RawMessage{}
		err := json.Unmarshal([]byte(message.Args), &rawArgs)
		if err != nil {
			return err
		}

		switch message.Var {

		case "pod.tzzh.kafka/new-admin-client":
			err := checkArgs("new-admin-client", rawArgs, 1)
			if err != nil {
				return err
			}
			config := &kafka.ConfigMap{}
			err = json.Unmarshal(rawArgs[0], &config)
			if err != nil {
				return err
			}

			adminClient, err := kafka.NewAdminClient(config)
			if err != nil {
				return err
			}
			adminClientName := adminClient.String()
			state[adminClientName] = adminClient
			babashka.WriteInvokeResponse(message, adminClientName)

		case "pod.tzzh.kafka/get-metadata":
			err := checkArgs("get-metadata", rawArgs, 1)
			if err != nil {
				return err
			}
			var adminClientName string
			err = json.Unmarshal(rawArgs[0], &adminClientName)
			if err != nil {
				return err
			}

			adminClient, err := getAdminClient(state, adminClientName)
			if err != nil {
				return err
			}

			metadata, err := adminClient.GetMetadata(nil, true, 5000)
			if err != nil {
				return err
			}
			babashka.WriteInvokeResponse(message, metadata)

		case "pod.tzzh.kafka/create-topics":
			err := checkArgs("create-topics", rawArgs, 2)
			if err != nil {
				return err
			}

			var adminClientName string
			err = json.Unmarshal(rawArgs[0], &adminClientName)
			if err != nil {
				return err
			}

			adminClient, err := getAdminClient(state, adminClientName)
			if err != nil {
				return err
			}

			topicSpecs := []kafka.TopicSpecification{}
			err = json.Unmarshal(rawArgs[1], &topicSpecs)
			if err != nil {
				return err
			}

			topicResult, err := adminClient.CreateTopics(context.TODO(), topicSpecs)
			if err != nil {
				return err
			}
			babashka.WriteInvokeResponse(message, topicResult)

		case "pod.tzzh.kafka/delete-topics":
			err := checkArgs("delete-topics", rawArgs, 2)
			if err != nil {
				return err
			}
			var adminClientName string
			err = json.Unmarshal(rawArgs[0], &adminClientName)
			if err != nil {
				return err
			}

			adminClient, err := getAdminClient(state, adminClientName)
			if err != nil {
				return err
			}

			topicNames := []string{}
			err = json.Unmarshal(rawArgs[1], &topicNames)
			if err != nil {
				return err
			}

			topicResult, err := adminClient.DeleteTopics(context.TODO(), topicNames)
			if err != nil {
				return err
			}
			babashka.WriteInvokeResponse(message, topicResult)

		case "pod.tzzh.kafka/new-producer":
			err := checkArgs("new-producer", rawArgs, 1)
			if err != nil {
				return err
			}
			config := &kafka.ConfigMap{}
			err = json.Unmarshal(rawArgs[0], &config)
			if err != nil {
				return err
			}

			producer, err := kafka.NewProducer(config)
			if err != nil {
				return err
			}
			producerName := producer.String()
			state[producerName] = producer
			babashka.WriteInvokeResponse(message, producerName)

		case "pod.tzzh.kafka/close-producer":
			err := checkArgs("close-producer", rawArgs, 1)
			if err != nil {
				return err
			}
			var producerName string
			err = json.Unmarshal(rawArgs[0], &producerName)
			if err != nil {
				return err
			}

			producer, err := getProducer(state, producerName)
			if err != nil {
				return err
			}

			producer.Close()
			delete(state, producerName)
			babashka.WriteInvokeResponse(message, nil)

		case "pod.tzzh.kafka/produce":
			err := checkArgs("produce", rawArgs, 4)
			if err != nil {
				return err
			}
			var producerName string
			err = json.Unmarshal(rawArgs[0], &producerName)
			if err != nil {
				return err
			}

			producer, err := getProducer(state, producerName)
			if err != nil {
				return err
			}

			var topic string
			err = json.Unmarshal(rawArgs[1], &topic)
			if err != nil {
				return err
			}

			var partitionP *int32
			err = json.Unmarshal(rawArgs[2], &partitionP)
			if err != nil {
				return err
			}
			var partition int32
			if partitionP == nil {
				partition = kafka.PartitionAny
			} else {
				partition = *partitionP
			}

			var value string
			err = json.Unmarshal(rawArgs[3], &value)
			if err != nil {
				return err
			}

			deliveryChan := make(chan kafka.Event)
			producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
				Value:          []byte(value),
				Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
			}, deliveryChan)
			e := <-deliveryChan
			m := e.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				return m.TopicPartition.Error
			}
			close(deliveryChan)

			babashka.WriteInvokeResponse(message, nil)

		case "pod.tzzh.kafka/new-consumer":
			err := checkArgs("new-consumer", rawArgs, 1)
			if err != nil {
				return err
			}
			config := &kafka.ConfigMap{}
			err = json.Unmarshal(rawArgs[0], &config)
			if err != nil {
				return err
			}

			consumer, err := kafka.NewConsumer(config)
			if err != nil {
				return err
			}
			consumerName := consumer.String()
			state[consumerName] = consumer
			babashka.WriteInvokeResponse(message, consumerName)

		case "pod.tzzh.kafka/close-consumer":
			err := checkArgs("close-consumer", rawArgs, 1)
			if err != nil {
				return err
			}
			var consumerName string
			err = json.Unmarshal(rawArgs[0], &consumerName)
			if err != nil {
				return err
			}

			consumer, err := getConsumer(state, consumerName)
			if err != nil {
				return err
			}

			err = consumer.Close()
			if err != nil {
				return err
			}
			delete(state, consumerName)
			babashka.WriteInvokeResponse(message, nil)

		case "pod.tzzh.kafka/subscribe-topics":
			err := checkArgs("subscribe-topics", rawArgs, 2)
			if err != nil {
				return err
			}
			var consumerName string
			err = json.Unmarshal(rawArgs[0], &consumerName)
			if err != nil {
				return err
			}

			consumer, err := getConsumer(state, consumerName)
			if err != nil {
				return err
			}

			topicNames := []string{}
			err = json.Unmarshal(rawArgs[1], &topicNames)
			if err != nil {
				return err
			}
			err = consumer.SubscribeTopics(topicNames, nil)
			if err != nil {
				return err
			}
			babashka.WriteInvokeResponse(message, nil)

		case "pod.tzzh.kafka/read-message":
			err := checkArgs("read-message", rawArgs, 2)
			if err != nil {
				return err
			}
			var consumerName string
			err = json.Unmarshal(rawArgs[0], &consumerName)
			if err != nil {
				return err
			}

			var pollDuration int
			err = json.Unmarshal(rawArgs[1], &pollDuration)
			if err != nil {
				return err
			}

			consumer, err := getConsumer(state, consumerName)
			if err != nil {
				return err
			}

			msg, err := consumer.ReadMessage(time.Duration(pollDuration) * time.Millisecond)
			if err != nil {
				kerr, ok := err.(kafka.Error)
				if ok && kerr.Code() == kafka.ErrTimedOut {
					// if the poll times out return nil
					babashka.WriteInvokeResponse(message, msg)
					return nil
				}
				return err
			}
			babashka.WriteInvokeResponse(message, msg)

		}
	}
	return nil
}
