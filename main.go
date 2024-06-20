package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

type Organizations struct {
	Organizations []Organization `json:"organizations"`
}

type Organization struct {
	OrganizationName string        `json:"organization_name"`
	Applications     []Application `json:"applications"`
}

type Application struct {
	ApplicationName string   `json:"application_name"`
	Devices         []Device `json:"devices"`
}

type Device struct {
	DeviceName string `json:"device_name"`
	DeviceType string `json:"device_type"`
	DeviceId   string `json:"device_id"`
}

func main() {
	id := uuid.New().String()
	var sbMqttClientIdSub strings.Builder
	var sbMqttClientIdPub strings.Builder
	var sbKafkaClientId strings.Builder
	// var sbPubTopic strings.Builder
	// var sbProdTopic strings.Builder
	sbMqttClientIdSub.WriteString("parse-lns-imt-mqtt-sub")
	sbMqttClientIdPub.WriteString("parse-lns-imt-mqtt-pub")
	sbKafkaClientId.WriteString("parse-lns-imt-mqtt-")
	sbMqttClientIdSub.WriteString(id)
	sbMqttClientIdPub.WriteString(id)
	sbKafkaClientId.WriteString(id)

	// MQTT
	sTopic := "lns_imt/+/rx"
	sBroker := "mqtt://mqtt.maua.br:1883"
	sClientId := sbMqttClientIdSub.String()
	sUser := "public"
	sPassword := "public"
	sQos := 0

	sOpts := MQTT.NewClientOptions()
	sOpts.AddBroker(sBroker)
	sOpts.SetClientID(sClientId)
	sOpts.SetUsername(sUser)
	sOpts.SetPassword(sPassword)

	pBroker := "mqtt://mqtt.maua.br:1883"
	pClientId := sbMqttClientIdPub.String()
	pUser := "public"
	pPassword := "public"
	// pQos := J0

	pOpts := MQTT.NewClientOptions()
	pOpts.AddBroker(pBroker)
	pOpts.SetClientID(pClientId)
	pOpts.SetUsername(pUser)
	pOpts.SetPassword(pPassword)

	c := make(chan [2]string)

	sOpts.SetDefaultPublishHandler(func(mqttClient MQTT.Client, msg MQTT.Message) {
		c <- [2]string{msg.Topic(), string(msg.Payload())}
	})

	sClient := MQTT.NewClient(sOpts)
	if token := sClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Printf("Connected to %s\n", sBroker)
	}

	// pClient := MQTT.NewClient(pOpts)
	// if token := pClient.Connect(); token.Wait() && token.Error() != nil {
	// 	panic(token.Error())
	// } else {
	// 	fmt.Printf("Connected to %s\n", pBroker)
	// }

	if token := sClient.Subscribe(sTopic, byte(sQos), nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	// KAFKA
	// p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "my-cluster-kafka-bootstrap.test-kafka.svc.cluster.local"})
	// if err != nil {
	// 	panic(err)
	// }
	// defer p.Close()

	// Delivery report handler for produced messages
	// go func() {
	// 	for e := range p.Events() {
	// 		switch ev := e.(type) {
	// 		case *kafka.Message:
	// 			if ev.TopicPartition.Error != nil {
	// 				fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
	// 			} else {
	// 				fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
	// 			}
	// 		}
	// 	}
	// }()

	// MQTT -> KAFKA
	for {
		incoming := <-c
		s := strings.Split(incoming[0], "/")
		devEUI := s[1]
		authorized := false

		fmt.Println("devEUI FROM TOPIC:" + devEUI)

		// IF DEVICE_ID IS REGISTRED, MOUNT THE CORRESPONDING TOPIC
		jsonFile, err := os.Open("schema.json")
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Successfully Opened schema.json")
		defer jsonFile.Close()
		byteValue, _ := io.ReadAll(jsonFile)
		var organizations Organizations
		json.Unmarshal(byteValue, &organizations)

		for i := 0; i < len(organizations.Organizations); i++ {
			for j := 0; j < len(organizations.Organizations[i].Applications); j++ {
				for k := 0; k < len(organizations.Organizations[i].Applications[j].Devices); k++ {
					fmt.Println(organizations.Organizations[i].OrganizationName + "/" + organizations.Organizations[i].Applications[j].ApplicationName + "/" + organizations.Organizations[i].Applications[j].Devices[k].DeviceType + "/" + organizations.Organizations[i].Applications[j].Devices[k].DeviceId)
					if devEUI == organizations.Organizations[i].Applications[j].Devices[k].DeviceId {
						authorized = true
					}
				}
			}
		}

		// sbPubTopic.Reset()
		// sbPubTopic.WriteString(schema.organizationName)
		// sbPubTopic.WriteString("/")
		// sbPubTopic.WriteString(lp.tags.applicationName)
		// sbPubTopic.WriteString("/")
		// sbPubTopic.WriteString(lp.tags.deviceType)
		// sbPubTopic.WriteString("/")
		// sbPubTopic.WriteString(lp.tags.deviceId)
		// sbPubTopic.WriteString("/")
		// sbPubTopic.WriteString("/rx")

		// sbProdTopic.Reset()
		// sbProdTopic.WriteString("lns_imt/")
		// sbProdTopic.WriteString(devEUI)
		// sbProdTopic.WriteString("/rx")

		// lp, err := decode.LoraImt(incoming[1])
		// if err != nil {
		// 	log.Panic(err)
		// }
		// fmt.Println(lp)

		// topic := "SmartCampusMaua.smartcampusmaua"

		// pClient.Publish(sbPubTopic.String(), byte(pQos), false, incoming[1])

		// p.Produce(&kafka.Message{
		// 	TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		// 	Value:          []byte(lp),
		// }, nil)

		// p.Flush(15 * 1000)
	}
}
