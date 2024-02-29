package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/OpenDataTelemetry/decode"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

func main() {
	id := uuid.New().String()
	var sbMqtt strings.Builder
	var sbKafka strings.Builder
	sbMqtt.WriteString("mqtt-")
	sbKafka.WriteString("kafka-")
	sbMqtt.WriteString(id)
	sbKafka.WriteString(id)

	// MQTT
	topic := "application/+/node/+/rx"
	broker := "mqtt://networkserver.maua.br:1883"
	password := "public"
	user := "PUBLIC"
	mqttClientId := sbMqtt.String()
	qos := 0

	opts := MQTT.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(mqttClientId)
	opts.SetUsername(user)
	opts.SetPassword(password)

	choke := make(chan [2]string)

	opts.SetDefaultPublishHandler(func(mqttClient MQTT.Client, msg MQTT.Message) {
		choke <- [2]string{msg.Topic(), string(msg.Payload())}
	})

	mqttClient := MQTT.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := mqttClient.Subscribe(topic, byte(qos), nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	// KAFKA
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "my-cluster-kafka-bootstrap.test-kafka.svc.cluster.local"})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// MQTT -> KAFKA
	for {
		incoming := <-choke
		lp, err := decode.LoraImt(incoming[1])
		if err != nil {
			log.Panic(err)
		}
		fmt.Println(lp)

		topic := "SmartCampusMaua.smartcampusmaua"

		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(lp),
		}, nil)

		p.Flush(15 * 1000)
	}
}
