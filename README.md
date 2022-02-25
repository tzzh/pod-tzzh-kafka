# pod-tzzh-kafka

A [pod](https://github.com/babashka/babashka.pods) to use Kafka with [babashka](https://github.com/borkdude/babashka/).

It's mainly useful for simple kafka admin tasks (e.g list/create/delete topics) or for producing/consuming a few messages on demand.

It's entirely based on [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go/), and exposes a small subset of the functions described in the [API docs](https://docs.confluent.io/current/clients/confluent-kafka-go/index.html). See the examples below for what's available and how to use it.


## Usage

### Loading the pod
`go build` the pod and then (I need to figure out how to setup goreleaser with CGO to add a proper release with binaries):
``` clojure
(require '[babashka.pods])
(babashka.pods/load-pod ["./pod-tzzh-kafka"])
(require '[pod.tzzh.kafka :as k])
```

### Config
The underlying go kafka library is itself based on librdkafka and therefore shares the same config options see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
``` clojure
(def admin-config
  {"bootstrap.servers" "broker1.com:9094,broker2.com:9094"
   "security.protocol" "ssl"
   "ssl.ca.location" "./certs/cacert"
   "ssl.key.location" "./certs/client_key"
   "ssl.certificate.location" "./certs/client_cert"})
```

### AdminClient
Call `new-admin-client` with a config argument to create an admin client on the pod.
The pod will return the name of the admin client that you can then pass back to the pod to use admin client functions.

``` clojure
(def admin-client 
  (k/new-admin-client admin-config))
;; e.g returns "admin-rdkafka#producer-1"

(k/get-metadata admin-client)

;; e.g to list all the topics
(-> (k/get-metadata admin-client)
    :Topics
    keys)

(k/create-topics admin-client [{:Topic "test-test-test"
                                :NumPartitions 2
                                :ReplicationFactor 3}])

(k/delete-topics admin-client ["test-test-test"])
```

### Producer
To create a producer call `new-producer` with a config and then you can produce messages synchronously by calling `produce` as shown below
``` clojure
(def producer (k/new-producer admin-config))

(k/produce producer "test-test-test" 0 "asdsadas")
;; the first argument is the topic name, second one is the partition (use nil for any partition) and third one is the value (only string for now)

(k/close-producer producer) ;; to decommission the producer
```

### Consumer
To create a consumer call `new-consumer` with a config and then you can consume message individually with `read-message`. The value is base64 encoded but can be decoded as `java.util.Base64` is part of babashka as shown in the example below.
``` clojure
(def consumer
  (k/new-consumer (merge admin-config {"group.id" "myGroup"
                                       "auto.offset.reset" "earliest"})))

(k/subscribe-topics consumer ["test-test-test"])

(k/read-message consumer 5000) ;; the argument is a timeout in ms and will return a message if there is one or nil if it times out

;; to decode a base64 encoded string value
(defn decode-base64-str
  [s]
  (-> (java.util.Base64/getDecoder) 
      (.decode s) 
      (String.)))

(some-> (k/read-message consumer 5000)
        :Value
        decode-base64-str)

(k/close-consumer consumer) ;; to commit final offsets and leave the consumer group
```

## Debugging

For debugging set the environment variable `POD_TZZH_KAFKA_DEBUG=true` and the logs will show in stderr.
