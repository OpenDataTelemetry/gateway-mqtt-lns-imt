package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/OpenDataTelemetry/decode"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

type LNS struct {
	DeviceId             string  `json:"LNS"`
	RxInfo_0_mac         string  `json:"rxInfo_0_mac"`
	RxInfo_0_time        string  `json:"rxInfo_0_time"`
	RxInfo_0_rssi        int64   `json:"rxInfo_0_rssi"`
	RxInfo_0_snr         float64 `json:"rxInfo_0_snr"`
	RxInfo_0_lat         float64 `json:"rxInfo_0_lat"`
	RxInfo_0_lon         float64 `json:"rxInfo_0_lon"`
	RxInfo_0_alt         int64   `json:"rxInfo_0_alt"`
	Tx_info_frequency    uint64  `json:"tx_info_frequency"`
	Tx_info_modulation   string  `json:"tx_info_modulation"`
	Tx_info_bandwidth    uint64  `json:"tx_info_bandwidth"`
	Tx_info_spreadFactor uint64  `json:"tx_info_spreadFactor"`
	Tx_info_codeRate     string  `json:"tx_info_codeRate"`
	FCnt                 uint64  `json:"fCnt"`
	FPort                uint64  `json:"fPort"`
	FType                string  `json:"fType"`
	Data                 string  `json:"data"`
}

type V2N struct {
}

type HealthPack struct {
}

type EVSE struct {
}
type Decoded struct {
	X_01   float64 `json:"01"`
	X_02   float64 `json:"02"`
	X_03_0 float64 `json:"03_0"`
	X_03_1 float64 `json:"03_1"`
	X_04   uint64  `json:"04"`
	X_05   uint64  `json:"05"`
	X_06   uint64  `json:"06"`
	X_07   uint64  `json:"07"`
	X_08   uint64  `json:"08"`
	X_09   uint64  `json:"09"`
	X_0A_0 float64 `json:"0A_0"`
	X_0A_1 float64 `json:"0A_1"`
	X_0B   uint64  `json:"0B"`
	X_0C   float64 `json:"0C"`
	X_0D_0 uint64  `json:"0D_0"`
	X_0D_1 uint64  `json:"0D_1"`
	X_0D_2 uint64  `json:"0D_2"`
	X_0D_3 uint64  `json:"0D_3"`
	X_0E_0 float64 `json:"0E_0"`
	X_0E_1 float64 `json:"0E_1"`
	X_10   uint64  `json:"10"`
	X_11   float64 `json:"11"`
	X_12   uint64  `json:"12"`
	X_13   uint64  `json:"13"`
}
type SmartLight struct {
	Temperature  float64 `json:"temperature"`
	Humidity     float64 `json:"humidity"`
	Lux          float64 `json:"lux"`
	Movement     uint64  `json:"movement"`
	Battery      float64 `json:"battery"`
	BoardVoltage float64 `json:"boardVoltage"`
}

type Imt struct {
	ApplicationID   string   `json:"applicationID"`
	ApplicationName string   `json:"applicationName"`
	NodeName        string   `json:"nodeName"`
	DevEUI          string   `json:"devEUI"`
	RxInfo          []RxInfo `json:"rxInfo"`
	TxInfo          TxInfo   `json:"txInfo"`
	FCnt            uint64   `json:"fCnt"`
	FPort           uint64   `json:"FPort"`
	Data            string   `json:"data"`
}

type RxInfo struct {
	Mac       string    `json:"mac"`
	Time      time.Time `json:"time"`
	Rssi      int64     `json:"rssi"`
	LoRaSNR   float64   `json:"loRaSNR"`
	Name      string    `json:"name"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Altitude  int64     `json:"altitude"`
}

type DataRate struct {
	Modulation   string `json:"modulation"`
	Bandwidth    uint64 `json:"bandwidth"`
	SpreadFactor uint64 `json:"spreadFactor"`
}

type TxInfo struct {
	Frequency uint64   `json:"frequency"`
	DataRate  DataRate `json:"dataRate"`
	Adr       bool     `json:"adr"`
	CodeRate  string   `json:"codeRate"`
}

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

// TODO:RECEIVE MQTT MESSAGE AND PARSE LNS_IMT TO LNS
func parseLnsImt(message string) string {
	if message == "" {
		// return message, errors.New("empty message to parse")
		return message
	}

	var lns LNS
	var lnsImt Imt
	var sb strings.Builder
	// var pd string // parsed data

	json.Unmarshal([]byte(message), &lnsImt)

	// TODO: JUST PARSE DATA TO LNS STRUCTURE
	lns.DeviceId = lnsImt.NodeName // string

	lns.Tx_info_frequency = lnsImt.TxInfo.Frequency                // int64
	lns.Tx_info_modulation = lnsImt.TxInfo.DataRate.Modulation     // string
	lns.Tx_info_bandwidth = lnsImt.TxInfo.DataRate.Bandwidth       // int64
	lns.Tx_info_spreadFactor = lnsImt.TxInfo.DataRate.SpreadFactor // int64
	lns.Tx_info_codeRate = lnsImt.TxInfo.CodeRate                  // string
	lns.FCnt = lnsImt.FCnt                                         // uint64
	lns.FPort = lnsImt.FPort                                       // uint64
	lns.FType = "uplink"                                           // string
	lns.Data = lnsImt.Data                                         // string

	// Show NodeName before Panic
	fmt.Printf("\n\nNodeName: %v\n", lnsImt.NodeName)
	// hex, _ := b64ToHex(lnsImt.Data)
	// fmt.Printf("Hex: %v\n", hex)

	// Set measurement
	deviceType := strings.Split(lnsImt.NodeName, "_")
	sb.WriteString(deviceType[0])
	sb.WriteString(",")

	// // Set tags
	// sb.WriteString("applicationName=")
	// sb.WriteString(lnsImt.ApplicationName)
	// sb.WriteString(",")

	// sb.WriteString("applicationID=")
	// sb.WriteString(lnsImt.ApplicationID)
	// sb.WriteString(",")

	// sb.WriteString("nodeName=")
	// sb.WriteString(lnsImt.NodeName)
	// sb.WriteString(",")

	// sb.WriteString("devEUI=")
	// sb.WriteString(lnsImt.DevEUI)
	// sb.WriteString(",")

	// Just for the first gateway
	for i, v := range lnsImt.RxInfo {

		if i == 0 {
			lns.RxInfo_0_mac = v.Mac            // string
			lns.RxInfo_0_time = v.Time.String() // string
			lns.RxInfo_0_rssi = v.Rssi          // int64
			lns.RxInfo_0_snr = v.LoRaSNR        // float64
			lns.RxInfo_0_lat = v.Latitude       // float64
			lns.RxInfo_0_lon = v.Longitude      // float64
			lns.RxInfo_0_alt = v.Altitude       // int64
			// sb.WriteString("lora_rxInfo_mac_")
			// sb.WriteString(strconv.FormatUint(uint64(i), 10))
			// sb.WriteString("=")
			// sb.WriteString(v.Mac)
			// sb.WriteString(",")

			// sb.WriteString("lora_rxInfo_name_")
			// sb.WriteString(strconv.FormatUint(uint64(i), 10))
			// sb.WriteString("=")
			// sb.WriteString(v.Name)
			// sb.WriteString(",")

			// sb.WriteString("rxInfo_time_")
			// sb.WriteString(strconv.FormatUint(uint64(i), 10))
			// sb.WriteString("=")
			// sb.WriteString(v.Time.String())
			// sb.WriteString(",")

			// sb.WriteString("lora_rxInfo_rssi_")
			// sb.WriteString(strconv.FormatUint(uint64(i), 10))
			// sb.WriteString("=")
			// sb.WriteString(strconv.FormatInt(v.Rssi, 10))
			// sb.WriteString(",")

			// sb.WriteString("lora_rxInfo_loRaSNR_")
			// sb.WriteString(strconv.FormatUint(uint64(i), 10))
			// sb.WriteString("=")
			// sb.WriteString(strconv.FormatInt(int64(v.LoRaSNR), 10))
			// sb.WriteString(",")

			// sb.WriteString("lora_rxInfo_latitude_")
			// sb.WriteString(strconv.FormatUint(uint64(i), 10))
			// sb.WriteString("=")
			// sb.WriteString(strconv.FormatFloat(v.Latitude, 'f', -1, 64))
			// sb.WriteString(",")

			// sb.WriteString("lora_rxInfo_longitude_")
			// sb.WriteString(strconv.FormatUint(uint64(i), 10))
			// sb.WriteString("=")
			// sb.WriteString(strconv.FormatFloat(v.Longitude, 'f', -1, 64))
			// sb.WriteString(",")

			// sb.WriteString("lora_rxInfo_altitude_")
			// sb.WriteString(strconv.FormatUint(uint64(i), 10))
			// sb.WriteString("=")
			// sb.WriteString(strconv.FormatInt(int64(v.Altitude), 10))
			// sb.WriteString(",")
		}
	}

	// sb.WriteString("lora_txInfo_dataRate_modulation=")
	// sb.WriteString(lnsImt.TxInfo.DataRate.Modulation)
	// sb.WriteString(",")

	// sb.WriteString("lora_txInfo_dataRate_bandwidth=")
	// sb.WriteString(strconv.FormatUint(lnsImt.TxInfo.DataRate.Bandwidth, 10))
	// sb.WriteString(",")

	// sb.WriteString("lora_txInfo_adr=")
	// sb.WriteString(strconv.FormatBool(lnsImt.TxInfo.Adr))
	// sb.WriteString(",")

	// sb.WriteString("lora_txInfo_codeRate=")
	// sb.WriteString(lnsImt.TxInfo.CodeRate)
	// sb.WriteString(",")

	// sb.WriteString("lora_fPort=")
	// sb.WriteString(strconv.FormatUint(lnsImt.FPort, 10))
	// sb.WriteString("")

	// Set fields
	// sb.WriteString(" ")

	// for i, v := range lnsImt.RxInfo {
	// if i == 0 {
	// sb.WriteString("rxInfo_time_")
	// sb.WriteString(strconv.FormatUint(uint64(i), 10))
	// sb.WriteString("=")
	// sb.WriteString(v.Time.String())
	// sb.WriteString(",")

	// sb.WriteString("lora_rxInfo_rssi_")
	// sb.WriteString(strconv.FormatUint(uint64(i), 10))
	// sb.WriteString("=")
	// sb.WriteString(strconv.FormatInt(v.Rssi, 10))
	// sb.WriteString(",")

	// sb.WriteString("lora_rxInfo_loRaSNR_")
	// sb.WriteString(strconv.FormatUint(uint64(i), 10))
	// sb.WriteString("=")
	// sb.WriteString(strconv.FormatInt(int64(v.LoRaSNR), 10))
	// sb.WriteString(",")

	// sb.WriteString("lora_rxInfo_latitude_")
	// sb.WriteString(strconv.FormatUint(uint64(i), 10))
	// sb.WriteString("=")
	// sb.WriteString(strconv.FormatFloat(v.Latitude, 'f', -1, 64))
	// sb.WriteString(",")

	// sb.WriteString("lora_rxInfo_longitude_")
	// sb.WriteString(strconv.FormatUint(uint64(i), 10))
	// sb.WriteString("=")
	// sb.WriteString(strconv.FormatFloat(v.Longitude, 'f', -1, 64))
	// sb.WriteString(",")

	// sb.WriteString("lora_rxInfo_altitude_")
	// sb.WriteString(strconv.FormatUint(uint64(i), 10))
	// sb.WriteString("=")
	// sb.WriteString(strconv.FormatInt(int64(v.Altitude), 10))
	// sb.WriteString(",")
	// }
	// }

	// sb.WriteString("lora_txInfo_frequency=")
	// sb.WriteString(strconv.FormatUint(lnsImt.TxInfo.Frequency, 10))
	// sb.WriteString(",")

	// sb.WriteString("lora_txInfo_dataRate_spreadFactor=")
	// sb.WriteString(strconv.FormatUint(lnsImt.TxInfo.DataRate.SpreadFactor, 10))
	// sb.WriteString(",")

	// Decode and Parse data
	// data := lnsImt.Data
	// b, err := b64ToByte(data)
	// if err != nil {
	// 	fmt.Print(lnsImt.Data)
	// 	log.Panic(err)
	// }

	// switch lnsImt.FPort {
	// case 100:
	// 	pd = imtIotProtocolParser(b)

	// 	// case 200:
	// 	// 	jsonProtocolParser()

	// 	// case 3:
	// 	// 	khompProtocolParser()

	// 	// case 4:
	// 	// 	khompProtocolParser()
	// }
	// sb.WriteString(pd)

	// sb.WriteString("fCnt=")
	// sb.WriteString(strconv.FormatUint(lnsImt.FCnt, 10))

	// Set time
	// sb.WriteString(" ")
	// t := time.Now().UnixNano()
	// sb.WriteString(strconv.FormatInt(t, 10))

	pd, err := json.Marshal(lns)
	if err != nil {
		fmt.Println(err)
		return "No parsed"
	}
	// fmt.Println(string(pd))
	// return sb.String(), nil
	return string(pd)
}

// CONVERT B64 to BYTE
func b64ToByte(b64 string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		log.Fatal(err)
	}
	return b, err
}

// RECEIVE DATA FROM PARSE LNS AND DECODE INTO _01
func decodeData(data string) string {
	if data == "" {
		return "No data"
	}

	// B64 to Byte
	b, err := b64ToByte(data)
	if err != nil {
		fmt.Print(data)
		log.Panic(err)
	}

	// var smartlight SmartLight
	// var decoded Decoded
	// var decodedData string

	// protocol parsed
	pp := decode.ImtIotProtocolParser(b)
	// fmt.Printf("PROTOCOL PARSED: [%x]", pp)
	// d is a json
	// decoded is a JSON string to be unmarshal according to deviceType
	// 01: TTTT
	// 02: HHHH
	// 0c: VVVV

	// json.Unmarshal([]byte(d), &decoded)
	// json.Unmarshal([]byte(d), &decoded)

	// switch deviceType {
	// case "SmartLight":
	// 	smartlight.Temperature = float64(decoded._01)
	// 	smartlight.Humidity = float64(decoded._02)
	// 	smartlight.Lux = float64(decoded._03)
	// 	smartlight.Movement = decoded._0D_1
	// 	smartlight.Battery = float64(decoded._0D_2)
	// 	smartlight.BoardVoltage = float64(decoded._0C)

	// 	decodedData, err = json.Marshal(smartlight)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 		return "SmartLight data wrongly decoded", err
	// 	}
	// default:
	// }

	// fmt.Println(string(pd))
	// return sb.String(), nil
	return string(pp)
}

func main() {
	id := uuid.New().String()
	var lns LNS
	var smartlight SmartLight
	var decodedData Decoded
	// var decodedDataJson []byte

	// var pd string // parsed data

	var lpd string
	// var ppd string
	var sbMqttClientIdSub strings.Builder
	var sbKafkaClientId strings.Builder
	var sbKafkaProducerTopic strings.Builder
	sbMqttClientIdSub.WriteString("parse-lns-lnsImt-mqtt-sub")
	sbKafkaClientId.WriteString("parse-lns-imt-mqtt-")
	sbMqttClientIdSub.WriteString(id)
	sbKafkaClientId.WriteString(id)

	// MQTT
	sTopic := "OpenDataTelemetry/IMT/+/+/rx/+"
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
		organization := s[1]
		deviceType := s[2]
		deviceId := s[3]
		lnsOrigin := s[5]
		sbKafkaProducerTopic.Reset()
		sbKafkaProducerTopic.WriteString(organization)
		sbKafkaProducerTopic.WriteString(".")
		sbKafkaProducerTopic.WriteString("SmartCampusMaua")

		fmt.Printf("Organization: %s\n", organization)
		fmt.Printf("KafkaTopic: %s\n", sbKafkaProducerTopic.String())
		fmt.Printf("deviceType: %s\n", deviceType)
		fmt.Printf("DeviceId: %s\n", deviceId)
		fmt.Printf("Message: %s\n", incoming[1])
		fmt.Printf("LNS: %s\n", lnsOrigin)
		fmt.Printf("\n")

		// Data TAG_KEYS shall be given by the application API using a Redis database. So the correct information shall be stored alongside with sensor
		// MAP deviceId vs deviceType to understand what decode really means for each one then write to kafka after decoded
		// Parse (IMT vs ATC) -> Map (deviceId vs deviceType) -> decode payload by port (0dCCCCCC)
		// -> measurement=,tag_key1=,tag_key2=, field_key_1= timestamp_ms
		// smartlight, raw=,temperature=,humidity=,lux=,movement=,box_battery=,board_voltage= timestamp_ms
		// gaugepressure, raw=pressure_in=,pressure_out=,board_voltage= timestamp_ms
		// watertanklevel, raw=distance=,pressure_out=,board_voltage= timestamp_ms
		// healthpack_alarm, raw=emergency= timestamp_ms
		// healthpack_tracking, raw=latitude=,longitude= timestamp_ms
		// evse_startTransaction, raw= timestamp_ms
		// evse_heartbeat, raw= timestamp_ms

		switch lnsOrigin {
		case "lns_imt":
			lpd = parseLnsImt(incoming[1]) // lns parsed data
			// if err != nil {
			// 	log.Panic(err)
			// }
			fmt.Printf("PARSED DATA: %s", lpd)
		case "lns_atc":
			// parseLnsAtc()
		default:
		}
		json.Unmarshal([]byte(lpd), &lns)
		// TODO: DECODE DATA ACCORDING MEASUREMENT THEN RETURN A JSON STRING
		dd := decodeData(lns.Data) // decoded data
		// dd: {"01":24.1,"02":95,"03_0":0,"03_1":0,"04":0,"05":0,"06":0,"07":0,"08":0,"09":0,"0A_0":0,"0A_1":0,"0B":518744,"0C":3.294,"0D_0":4095,"0D_1":0,"0D_2":0,"0D_3":0,"0E_0":0,"0E_1":0,"10":0,"11":0,"12":0,"13":0}
		// if err != nil {
		// }
		fmt.Printf("\n\n### PROTOCOL PARSED: %s", dd)

		var decodedDataJson []byte

		switch deviceType {
		case "SmartLight":
			json.Unmarshal([]byte(dd), &decodedData)

			smartlight.Temperature = float64(decodedData.X_01)
			smartlight.Humidity = float64(decodedData.X_02)
			smartlight.Lux = float64(decodedData.X_03_0)
			smartlight.Movement = decodedData.X_0D_0
			smartlight.Battery = float64(decodedData.X_0D_1)
			smartlight.BoardVoltage = float64(decodedData.X_0C)

			var err any
			decodedDataJson, err = json.Marshal(smartlight)
			if err != nil {
				fmt.Println(err)
			}
		default:
		}

		fmt.Printf("\n\n###ecodedDataJson:%s", string(decodedDataJson))

		// TODO: JSON TO INFLUX

		// FINISH

		// fmt.Println("topic:" + incoming[0])

		// sbKafkaProducerTopic.Reset()
		// sbKafkaProducerTopic.WriteString("lns_imt/")
		// sbKafkaProducerTopic.WriteString(devEUI)
		// sbKafkaProducerTopic.WriteString("/rx")

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
