package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

type Influx struct {
	Measurement string `json:"measurement"`
	Tags        any    `json:"tags"`
	Fields      any    `json:"fields"`
	Timestamp   uint64 `json:"timestamp"`
}
type LnsUp struct {
	Measurement        string  // `json:"measurement"`
	DeviceId           string  // `json:"deviceId"`
	RxInfoMac_0        string  // `json:"rxInfo_mac_0"`
	RxInfoTime_0       int64   // `json:"rxInfo_time_0"`
	RxInfoRssi_0       int64   // `json:"rxInfo_rssi_0"`
	RxInfoSnr_0        float64 // `json:"rxInfo_snr_0"`
	RxInfoLat_0        float64 // `json:"rxInfo_lat_0"`
	RxInfoLon_0        float64 // `json:"rxInfo_lon_0"`
	RxInfoAlt_0        uint64  // `json:"rxInfo_alt_0"`
	TxInfoFrequency    float64 // `json:"txInfo_frequency"`
	TxInfoModulation   string  // `json:"txInfo_modulation"`
	TxInfoBandWidth    uint64  // `json:"txInfo_bandwidth"`
	TxInfoSpreadFactor uint64  // `json:"txInfo_spreadFactor"`
	// TxInfoCodeRate     string  `json:"txInfo_codeRate"`
	FCnt  uint64 `json:"fCnt"`
	FPort uint64 `json:"fPort"`
	FType string `json:"fType"`
	Data  string `json:"data"`
}

type LnsDown struct {
	Measurement string
	Application string
	Reference   string
	DeviceId    string
	Confirmed   bool
	FPort       uint64
	Data        string
	Timestamp   int64
	// Object any
}

type LnsImtDown struct {
	Reference string
	Confirmed bool
	FPort     uint64
	Data      string
}

type LnsChirpstackV4Down struct {
	DeviceId  string
	Confirmed bool
	FPort     uint64
	Data      string
	// Object any
}
type Evse struct {
}
type Port100 struct {
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

type Port4 struct {
	InternalBatteryVoltage float64
	PowerSource            bool
	FirmwareVersion        uint64
	EnvSensorFailStatus    bool
	C1State                bool
	C1Count                uint64
	C2State                bool
	C2Count                uint64
	InternalTemperature    float64
	InternalHumidity       float64
	EmwRainLevel           float64
	EmwAvgWindSpeed        uint64
	EmwGustWindSpeed       uint64
	EmwWindDirection       uint64
	EmwTemperature         float64
	EmwHumidity            uint64
	EmwLuminosity          uint64
	EmwUv                  float64
	EmwSolarRadiation      float64
	EmwAtmPres             float64

	IsEnvSensorFailStatus    bool
	IsInternalBatteryVoltage bool
	IsFirmwareVersion        bool
	IsInternalTemperature    bool
	IsInternalHumidity       bool
	IsC1State                bool
	IsC1Count                bool
	IsC2State                bool
	IsC2Count                bool
	IsEmwRainLevel           bool
	IsEmwAvgWindSpeed        bool
	IsEmwGustWindSpeed       bool
	IsEmwWindDirection       bool
	IsEmwTemperature         bool
	IsEmwHumidity            bool
	IsEmwLuminosity          bool
	IsEmwUv                  bool
	IsEmwSolarRadiation      bool
	IsEmwAtmPres             bool
}
type SmartLight struct {
	Temperature    float64 `json:"temperature"`
	Humidity       float64 `json:"humidity"`
	Luminosity     float64 `json:"lux"`
	Movement       uint64  `json:"movement"`
	BatteryVoltage float64 `json:"battery"`
	BoardVoltage   float64 `json:"boardVoltage"`
}

type WaterTankLevel struct {
	Distance     uint64  `json:"distance"`
	BoardVoltage float64 `json:"boardVoltage"`
}

type WeatherStation struct {
	InternalBatteryVoltage float64
	FirmwareVersion        uint64
	EnvSensorFailStatus    bool
	C1State                bool
	C1Count                uint64
	C2State                bool
	C2Count                uint64
	InternalTemperature    float64
	InternalHumidity       float64
	EmwRainLevel           float64
	EmwAvgWindSpeed        uint64
	EmwGustWindSpeed       uint64
	EmwWindDirection       uint64
	EmwTemperature         float64
	EmwHumidity            uint64
	EmwLuminosity          uint64
	EmwUv                  float64
	EmwSolarRadiation      float64
	EmwAtmPres             float64
	PowerSource            bool
}

type GaugePressure struct {
	InletPressure  float64 `json:"outletPressure"`
	OutletPressure float64 `json:"inletPressure"`
	BoardVoltage   float64 `json:"boardVoltage"`
}

type Hydrometer struct {
	Counter      uint64  `json:"counter"`
	BoardVoltage float64 `json:"boardVoltage"`
}

type EnergyMeter struct {
	ForwardEnergy float64 `json:"forwardEnergy"`
	ReverseEnergy float64 `json:"reverseEnergy"`
	BoardVoltage  float64 `json:"boardVoltage"`
}

type LnsChirpStackV4Up struct {
	DeduplicationId string                      `json:"deduplicationId"`
	DeviceInfo      LnsChirpStackV4UpDeviceInfo `json:"deviceInfo"`
	DevAddr         string                      `json:"devAddr"`
	Adr             bool                        `json:"adr"`
	Dr              uint64                      `json:"dr"`
	FCnt            uint64                      `json:"fCnt"`
	FPort           uint64                      `json:"fPort"`
	Confirmed       string                      `json:"Confirmed"`
	RxInfo          []LnsChirpStackV4UpRxInfo   `json:"rxInfo"`
	TxInfo          LnsChirpStackV4UpTxInfo     `json:"txInfo"`
	Data            string                      `json:"data"`
}

type LnsChirpStackV4UpDeviceInfo struct {
	TenantId           string `json:"tenantId"`
	TenantName         string `json:"tenantName"`
	ApplicationId      string `json:"applicationId"`
	ApplicationName    string `json:"applicationName"`
	DeviceProfileId    string `json:"deviceProfileId"`
	DeviceProfileName  string `json:"deviceProfileName"`
	DeviceName         string `json:"deviceName"`
	DevEui             string `json:"devEui"`
	DeviceClassEnabled string `json:"deviceClassEnabled"`
	Tags               any    `json:"tags"`
}

type LnsChirpStackV4UpRxInfo struct {
	GatewayId         string                    `json:"gatewayId"`
	UplinkId          uint64                    `json:"uplinkId"`
	NsTime            time.Time                 `json:"nsTime"`
	TimeSinceGpsEpoch string                    `json:"timeSinceGpsEpoch"`
	Rssi              int64                     `json:"rssi"`
	Snr               float64                   `json:"snr"`
	Channel           uint64                    `json:"channel"`
	Board             uint64                    `json:"board"`
	Location          LnsChirpStackV4UpLocation `json:"location"`
	Context           string                    `json:"context"`
	Metadata          LnsChirpStackV4UpMetadata `json:"metadata"`
	CrcStatus         string                    `json:"crcStatus"`
}

type LnsChirpStackV4UpLocation struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Altitude  uint64  `json:"altitude"`
}

type LnsChirpStackV4UpMetadata struct {
	Region_config_id   string `json:"region_config_id"`
	Region_common_name string `json:"region_common_name"`
}

type LnsChirpStackV4UpTxInfo struct {
	Frequency  float64                     `json:"frequency"`
	Modulation LnsChirpStackV4UpModulation `json:"modulation"`
}

type LnsChirpStackV4UpModulation struct {
	Lora LnsChirpStackV4UpLora `json:"lora"`
}

type LnsChirpStackV4UpLora struct {
	Bandwidth       uint64 `json:"bandwidth"`
	SpreadingFactor uint64 `json:"spreadingFactor"`
	// CodeRate        string `json:"codeRate"`
}

type LnsImtUp struct {
	ApplicationID   string           `json:"applicationID"`
	ApplicationName string           `json:"applicationName"`
	NodeName        string           `json:"nodeName"`
	DevEUI          string           `json:"devEUI"`
	RxInfo          []LnsImtUpRxInfo `json:"rxInfo"`
	TxInfo          LnsImtUpTxInfo   `json:"txInfo"`
	FCnt            uint64           `json:"fCnt"`
	FPort           uint64           `json:"FPort"`
	Data            string           `json:"data"`
}

type LnsImtUpRxInfo struct {
	Mac       string    `json:"mac"`
	Time      time.Time `json:"time"`
	Rssi      int64     `json:"rssi"`
	LoRaSNR   float64   `json:"loRaSNR"`
	Name      string    `json:"name"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Altitude  uint64    `json:"altitude"`
}

type LnsImtUpDataRate struct {
	Modulation   string `json:"modulation"`
	Bandwidth    uint64 `json:"bandwidth"`
	SpreadFactor uint64 `json:"spreadFactor"`
}

type LnsImtUpTxInfo struct {
	Frequency float64          `json:"frequency"`
	DataRate  LnsImtUpDataRate `json:"dataRate"`
	Adr       bool             `json:"adr"`
	// CodeRate  string         `json:"codeRate"`
}

func roundFloat(val float64, precision uint) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}

func protocolParserPort4(bytes []byte) string {
	var port4 Port4
	port4.IsInternalTemperature = false
	port4.IsInternalHumidity = false
	port4.IsEmwRainLevel = false
	port4.IsEmwAvgWindSpeed = false
	port4.IsEmwGustWindSpeed = false
	port4.IsEmwWindDirection = false
	port4.IsEmwTemperature = false
	port4.IsEmwHumidity = false
	port4.IsEmwLuminosity = false
	port4.IsEmwUv = false
	port4.IsEmwSolarRadiation = false
	port4.IsEmwAtmPres = false
	port4.IsEnvSensorFailStatus = false
	port4.IsInternalBatteryVoltage = false
	port4.IsFirmwareVersion = false
	port4.IsC1State = false
	port4.IsC1Count = false
	port4.IsC2State = false
	port4.IsC2Count = false

	var maskSensorInt byte
	var maskSensorIntE byte
	var maskSensorExt byte

	index := 0

	// deviceModel := "NIT 21LI"

	// Verify the presence os maskSensorInt in byte[0]
	maskSensorInt = bytes[index]
	// fmt.Printf("\nprotocolParserPort4 => maskSensorInt %b", maskSensorInt)
	index = index + 1
	// If Extended Internal Sensor Mask
	if maskSensorInt>>7&0x01 == 0x01 {
		maskSensorIntE = bytes[index]
		port4.IsEnvSensorFailStatus = true
		// fmt.Printf("\nprotocolParserPort4 => maskSensorIntE %b", maskSensorIntE)
		index = index + 1
	}

	// External Sensor Mask
	// byte [3] if Extended Internal Sensor Mask or byte [2] if does not
	maskSensorExt = bytes[index]
	// fmt.Printf("\nprotocolParserPort4 => maskSensorExt %b", maskSensorExt)
	index = index + 1

	// Verify Internal Humidity and Temperature fails
	if maskSensorIntE>>0&0x01 == 0x01 {
		// if 0x01&0x01 == 0x01 {
		port4.EnvSensorFailStatus = true
		// fmt.Printf("\nprotocolParserPort4 => EnvSensorFailStatus %s", port4.EnvSensorFailStatus)
	}

	// TODO: VERIFY CODE
	// Decode Battery
	// If bit 0 of maskSensorInt exists
	if maskSensorInt>>0&0x01 == 0x01 {
		port4.IsInternalBatteryVoltage = true
		if maskSensorInt>>6&0x01 == 0x01 {
			v := bytes[index]
			f := roundFloat(float64((v/120.0)+1), 2)
			port4.InternalBatteryVoltage = f
		} else {
			v := bytes[index]
			f := roundFloat(float64(v/10.0), 1)
			port4.InternalBatteryVoltage = f
		}
		index = index + 1
	}
	// fmt.Printf("\nprotocolParserPort4 => IsBattery %d", port4.IsBattery)

	// TODO: VERIFY CODE WITH 0xFE
	// Decode Firmware Version
	// Verify if firmware version appear on message
	if maskSensorInt>>2&0x01 == 0x01 {
		port4.IsFirmwareVersion = true
		v := uint64(bytes[index])
		v |= uint64(bytes[index+2]) << 8
		v |= uint64(bytes[index+3]) << 16
		v = v / 1000000
		// fmt.Printf("\nprotocolParserPort4 => Firmware Version %d", v)

		// hardware = (v / 1000000) >>> 0;
		// let compatibility = ((firmware.v / 10000) - (hardware * 100)) >>> 0;
		// let feature = ((firmware.v - (hardware * 1000000) - (compatibility * 10000)) / 100) >>> 0;
		// let bug = (firmware.v - (hardware * 1000000) - (compatibility * 10000) - (feature * 100)) >>> 0;
		// firmware.v = hardware + '.' + compatibility + '.' + feature + '.' + bug;
		// data.device.push(firmware);
		index = index + 3
	}
	// fmt.Printf("\nprotocolParserPort4 => IsFirmware %d", port4.IsFirmware)

	// Decode External Power or Battery
	if maskSensorInt>>5&0x01 == 0x01 {
		b := true
		port4.PowerSource = b
	} else {
		b := false
		port4.PowerSource = b
	}
	// fmt.Printf("\nprotocolParserPort4 => Power Source %d", port4.Power)

	// Decode Temperature Int
	if maskSensorInt>>3&0x01 == 0x01 {
		port4.IsInternalTemperature = true
		v := uint64(bytes[index])
		v |= uint64(bytes[index+1]) << 8
		f := roundFloat((float64(v)/100)-273.15, 2)
		port4.InternalTemperature = f
		index = index + 2
		// fmt.Printf("\nprotocolParserPort4 => Internal Temperature f %d", f)
	}

	// Decode Moisture Int
	if maskSensorInt>>4&0x01 == 0x01 {
		port4.IsInternalHumidity = true
		v := uint64(bytes[index])
		v |= uint64(bytes[index+1]) << 8
		f := roundFloat((float64(v) / 10), 2)
		port4.InternalHumidity = f
		index = index + 2
		// fmt.Printf("\nprotocolParserPort4 => Internal Humidity f %d", f)
	}

	// Decode Drys
	// Decode Dry 1 State
	if maskSensorExt>>0&0x01 == 0x01 {
		port4.IsC1State = true
		if bytes[index] == 0x01 {
			b := true
			port4.C1State = b
		} else {
			b := false
			port4.C1State = b
		}
		index = index + 1
		// fmt.Printf("\nprotocolParserPort4 => C1State %d", port4.C1State)
	}

	// Decode Dry 1 Count
	if maskSensorExt>>1&0x01 == 0x01 {
		port4.IsC1Count = true
		v := uint64(bytes[index])
		v |= uint64(bytes[index+1]) << 8
		port4.C1Count = v
		index = index + 2
		// fmt.Printf("\nprotocolParserPort4 => C1Count %d", port4.C1Count)
	}

	// Decode Dry 2 State
	if maskSensorExt>>2&0x01 == 0x01 {
		port4.IsC2State = true
		if bytes[index] == 0x01 {
			b := true
			port4.C2State = b
		} else {
			b := false
			port4.C2State = b
		}
		index = index + 1
		// fmt.Printf("\nprotocolParserPort4 => C2State %d", port4.C2State)
	}

	// Decode Dry 2 Count
	if maskSensorExt>>3&0x01 == 0x01 {
		port4.IsC2Count = true
		v := uint64(bytes[index])
		v |= uint64(bytes[index+1]) << 8
		port4.C2Count = v
		index = index + 2
		// fmt.Printf("\nprotocolParserPort4 => C2Count %d", port4.C2Count)
	}

	// // Decode DS18B20 Probe
	// if (mask_sensor_ext >> 4 & 0x07 == 0x07) {
	// 		let nb_probes = (mask_sensor_ext >> 4 & 0x07) >>> 0;
	// 		for (let i = 0; i < nb_probes; i++) {
	// 				let probe = { u: 'C' };
	// 				let rom = {};
	//
	// 				probe.v = (((input.bytes[index++] | (input.bytes[index++] << 8)) / 100.0) - 273.15).round(2);
	// 				if (mask_sensor_ext >> 7 & 0x01) {
	// 						index += 7;
	// 						rom = (input.bytes[index--]).toString(16);
	// 						for (let j = 0; j < 7; j++) {
	// 								rom += (input.bytes[index--]).toString(16);
	// 						}
	// 						index += 9;
	// 				} else {
	// 						rom = input.bytes[index++];
	// 				}
	// 				probe.n = 'temperature' + '_' + rom;
	// 				data.probes.push(probe);
	// 		}
	// }

	// Decode Extension Module(s) ONLY EMW104 validated
	if bytes[index] == 0x04 {

		switch bytes[index] {
		// 		case 1:
		// 			i = i + 1
		// 			maskEms104 := bytes[index+i]
		// 			i = i + 1

		// 			// E1
		// 			if maskEms104>>0&0x01 == 0x01 {
		// 				v := uint64(bytes[index])
		// 				v |= uint64(bytes[index+1] << 8)
		// 				f := float64(v / 100.0)
		// 				// .round(2)
		// 				port4.EmsE1Temp = f - 273.15
		// 			}

		// 			// KPA
		// 			// if (maskEms104 >> (k + 1) & 0x01) {
		// 			switch _kpa {
		// 			case 0:
		// 				v := uint64(bytes[i+3])
		// 				v |= uint64(bytes[i+4]) << 8
		// 				f := float64(v)
		// 				port100.Kpa_0 = f
		// 				i = i + 2
		// 				_kpa = _kpa + 1
		// 			case 1:
		// 				v := uint64(bytes[i+3])
		// 				v |= uint64(bytes[i+4]) << 8
		// 				f := float64(v)
		// 				port100.Kpa_1 = f
		// 				i = i + 2
		// 				_kpa = _kpa + 1
		// 			case 2:
		// 				v := uint64(bytes[i+3])
		// 				v |= uint64(bytes[i+4]) << 8
		// 				f := float64(v)
		// 				port100.Kpa_2 = f
		// 				i = i + 2
		// 				_kpa = _kpa + 1
		// 			}
		// 		case 2:

		// EM W104
		case 4:
			index = index + 1
			maskEmw104 := bytes[index] //0f
			index = index + 1
			// fmt.Printf("\nprotocolParserPort4 => maskEmw104 %d", maskEmw104)

			//Weather Station
			if maskEmw104>>0&0x01 == 0x01 {
				//Rain
				port4.IsEmwRainLevel = true
				v := uint64(bytes[index]) << 8
				v |= uint64(bytes[index+1])
				f := roundFloat((float64(v) / 10), 1)
				port4.EmwRainLevel = f
				index = index + 2
				// fmt.Printf("\nprotocolParserPort4 => EmwRainLevel %d", port4.EmwRainLevel)

				//Average Wind Speed
				port4.IsEmwAvgWindSpeed = true
				v = uint64(bytes[index])
				port4.EmwAvgWindSpeed = v
				// fmt.Printf("\nprotocolParserPort4 => EmwAvgWindSpeed %d", port4.EmwAvgWindSpeed)
				index = index + 1

				//Gust Wind Speed
				port4.IsEmwGustWindSpeed = true
				v = uint64(bytes[index])
				port4.EmwGustWindSpeed = v
				// fmt.Printf("\nprotocolParserPort4 => EmwGustWindSpeed %d", port4.EmwGustWindSpeed)
				index = index + 1

				//Wind Direction
				port4.IsEmwWindDirection = true
				v = uint64(bytes[index]) << 8
				v |= uint64(bytes[index+1])
				port4.EmwWindDirection = v
				// fmt.Printf("\nprotocolParserPort4 => EmwWindDirection %d", port4.EmwWindDirection)
				index = index + 2

				//Temperature
				port4.IsEmwTemperature = true
				v = uint64(bytes[index]) << 8
				v |= uint64(bytes[index+1])
				f = roundFloat((float64(v)/10)-273.15, 2)
				port4.EmwTemperature = f
				// fmt.Printf("\nprotocolParserPort4 => EmwTemperature %d", port4.EmwTemperature)
				index = index + 2

				//Humidity
				port4.IsEmwHumidity = true
				v = uint64(bytes[index])
				port4.EmwHumidity = v
				// fmt.Printf("\nprotocolParserPort4 => EmwHumidity %d", port4.EmwHumidity)
				index = index + 1
			}
			//Lux and UV
			if maskEmw104>>1&0x01 == 0x01 {
				port4.IsEmwLuminosity = true
				v := uint64(bytes[index]) << 16
				v |= uint64(bytes[index+1]) << 8
				v |= uint64(bytes[index+2])
				port4.EmwLuminosity = v
				// fmt.Printf("\nprotocolParserPort4 => EmwLuminosity %d", port4.EmwLuminosity)

				port4.IsEmwUv = true
				v = uint64(bytes[index+3])
				f := roundFloat((float64(v) / 10), 1)
				port4.EmwUv = f
				// fmt.Printf("\nprotocolParserPort4 => EmwUv %d", port4.EmwUv)
				index = index + 4
			}

			//Pyranometer
			if maskEmw104>>2&0x01 == 0x01 {
				port4.IsEmwSolarRadiation = true
				v := uint64(bytes[index]) << 8
				v |= uint64(bytes[index+1])
				f := roundFloat((float64(v) / 10), 1)
				port4.EmwSolarRadiation = f
				// fmt.Printf("\nprotocolParserPort4 => EmwSolarRadiation %d", port4.EmwSolarRadiation)
				index = index + 2
			}

			//Barometer
			if maskEmw104>>3&0x01 == 0x01 {
				port4.IsEmwAtmPres = true
				v := uint64(bytes[index]) << 16
				v |= uint64(bytes[index+1]) << 8
				v |= uint64(bytes[index+2])
				f := roundFloat((float64(v) / 100), 2)
				port4.EmwAtmPres = f
				// fmt.Printf("\nprotocolParserPort4 => EmwAtmPres %d", port4.EmwAtmPres)
				index = index + 3
			}

		// 			// EM R102
		// 			// case 5:

		// 			// EM ACW100 & EM THW 100/200/201
		// 			// case 6:

		default:
			// fmt.Print("Data not parsed by decode.LoRaImt imtIotProtocolParser()\n")
			fmt.Print("Data not parsed default PORT4\n")
			// break PL

			// }
		}
	}
	p, err := json.Marshal(port4)
	if err != nil {
		fmt.Println(err)
		return "Port4 data parsed wrongly"
	}
	return string(p[:])
}

func protocolParserPort100(bytes []byte) string {
	var port100 Port100

	len := len(bytes)
	_0d := 0
	_0e := 0
	_03 := 0

PL: // Parse Loop
	for i := 0; i < len; i++ {
		switch bytes[i] {
		// case 0x00:
		// fmt.Println("00")

		case 0x01:
			v := uint64(bytes[i+1]) << 8
			v |= uint64(bytes[i+2])
			f := float64(v) / 10
			i = i + 2
			port100.X_01 = f

		case 0x02:
			v := uint64(bytes[i+1]) << 8
			v |= uint64(bytes[i+2])
			f := float64(v) / 10
			i = i + 2
			port100.X_02 = f

		case 0x03:
			switch _03 {
			case 0:
				v := uint64(bytes[i+3]) << 8
				v |= uint64(bytes[i+4])
				f := float64(v)
				port100.X_03_0 = f
				i = i + 2
				_03 = _03 + 1
			case 1:
				v := uint64(bytes[i+3]) << 8
				v |= uint64(bytes[i+4])
				f := float64(v)
				port100.X_03_1 = f
				i = i + 2
				_03 = _03 + 1
			}

			// case 0x03:
			//   var press = {};
			//   press.v = (bytes[index++]<<8) | bytes[index++];
			//   press.n = "press";
			//   press.u = "hPa";
			//   decoded.modules.push(press);
			//   break;

			// 	// case 0x04:
			// 	//   var corrente = {};
			// 	//   corrente.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   corrente.n = "corrente";
			// 	//   corrente.u = "A";
			// 	//   decoded.modules.push(corrente);
			// 	//   break;

			// 	// case 0x05:
			// 	//   var gyrox = {};
			// 	//   gyrox.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   gyrox.n = "GiroscopioX";
			// 	//   gyrox.u = "g";
			// 	//   decoded.modules.push(gyrox);
			// 	//   var gyroy = {};
			// 	//   gyroy.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   gyroy.n = "GiroscopioY";
			// 	//   gyroy.u = "g";
			// 	//   decoded.modules.push(gyroy);
			// 	//   var gyroz = {};
			// 	//   gyroz.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   gyroz.n = "GiroscopioZ";
			// 	//   gyroz.u = "g";
			// 	//   decoded.modules.push(gyroz);
			// 	//   break;

			// 	// case 0x06:
			// 	//   var accx = {};
			// 	//   accx.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   accx.n = "AceleromeroX";
			// 	//   accx.u = "g";
			// 	//   decoded.modules.push(accx);
			// 	//   var accy = {};
			// 	//   accy.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   accy.n = "AceleromeroY";
			// 	//   accy.u = "g";
			// 	//   decoded.modules.push(accy);
			// 	//   var accz = {};
			// 	//   accz.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   accz.n = "AceleromeroZ";
			// 	//   accz.u = "g";
			// 	//   decoded.modules.push(accz);
			// 	//   break;

			// 	// case 0x07:
			// 	//   var magx = {};
			// 	//   magx.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   magx.n = "MagnetometroX";
			// 	//   magx.u = "mGauss";
			// 	//   decoded.modules.push(magx);
			// 	//   var magy = {};
			// 	//   magy.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   magy.n = "MagnetometroY";
			// 	//   magy.u = "mGauss";
			// 	//   decoded.modules.push(magy);
			// 	//   var magz = {};
			// 	//   magz.v = (bytes[index++]<<8) | bytes[index++];
			// 	//   magz.n = "MagnetometroZ";
			// 	//   magz.u = "mGauss";
			// 	//   decoded.modules.push(magz);
			// 	//   break;

			// 	// case 0x08:
			// 	//     //data.rtc = data.remainingData.slice(0,6);
			// 	//     bytes[index++];bytes[index++];
			// 	//     bytes[index++];
			// 	//     bytes[index++];
			// 	//     break;

			// 	// case 0x09:
			// 	//     //data.date = data.remainingData.slice(0,8);
			// 	//     bytes[index++];bytes[index++];
			// 	//     bytes[index++];bytes[index++];

			// 	//     break;

		case 0x0A:
			var f float64
			v := uint64(bytes[i+1])
			a := uint64(bytes[i+2]) << 16
			a |= uint64(bytes[i+3]) << 8
			a |= uint64(bytes[i+4])
			b := float64(a) / 1000000

			if v > 127 {
				f = -((255 - float64(v)) + 1) - b //complement of 2
			} else {
				f = float64(v) + b
			}
			port100.X_0A_0 = f

			v = uint64(bytes[i+5])
			a = uint64(bytes[i+6]) << 16
			a |= uint64(bytes[i+7]) << 8
			a |= uint64(bytes[i+8])
			b = float64(a) / 1000000

			if v > 127 {
				f = -((255 - float64(v)) + 1) - b //complement of 2
			} else {
				f = float64(v) + b
			}
			port100.X_0A_1 = f
			i = i + 8

		case 0x0B:
			v := uint64(bytes[i+1]) << 16
			v |= uint64(bytes[i+2]) << 8
			v |= uint64(bytes[i+3])
			port100.X_0B = v
			i = i + 3

		case 0x0C:
			v := uint64(bytes[i+1]) << 8
			v |= uint64(bytes[i+2])
			f := float64(v) / 1000
			port100.X_0C = f
			i = i + 2

		case 0x0D:
			switch _0d {
			case 0:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				port100.X_0D_0 = v
				i = i + 2
				_0d = _0d + 1

			case 1:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				port100.X_0D_1 = v
				i = i + 2
				_0d = _0d + 1

			case 2:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				port100.X_0D_2 = v
				i = i + 2
				_0d = _0d + 1

			case 3:
				v := uint64(bytes[i+1]) << 8
				v |= uint64(bytes[i+2])
				port100.X_0D_3 = v
				i = i + 2
				_0d = _0d + 1
			}

		case 0x0E:
			switch _0e {
			case 0:
				v := uint64(bytes[i+1]) << 24
				v |= uint64(bytes[i+2]) << 16
				v |= uint64(bytes[i+3]) << 8
				v |= uint64(bytes[i+4])
				f := float64(v) * (150 / 5) / 2000
				port100.X_0E_0 = f
				i = i + 4
				_0e = _0e + 1

			case 1:
				v := uint64(bytes[i+1]) << 24
				v |= uint64(bytes[i+2]) << 16
				v |= uint64(bytes[i+3]) << 8
				v |= uint64(bytes[i+4])
				f := float64(v) * (150 / 5) / 2000
				port100.X_0E_1 = f
				i = i + 4
				_0e = _0e + 1
			}
			// 	// case 0x0F:
			// 	//     //data.rfid = data.remainingData.slice(0,16);
			// 	//     bytes[index++];bytes[index++];
			// 	//     bytes[index++];bytes[index++];
			// 	//     bytes[index++];bytes[index++];
			// 	//     bytes[index++];bytes[index++];
			// 	//     break;

		case 0x10:
			v := uint64(bytes[i+1]) << 8
			v |= uint64(bytes[i+2])
			port100.X_10 = v
			i = i + 2

		case 0x11:
			v := uint64(bytes[i+1]) << 8
			v |= uint64(bytes[i+2])
			f := float64(v) / 100
			port100.X_11 = f
			i = i + 2

			// 	// case 0x12:
			// 	//     //data.color = data.remainingData.slice(0,4);
			// 	//     bytes[index++];bytes[index++];
			// 	//     break;

		case 0x13:
			v := uint64(bytes[i+1]) << 8
			v |= uint64(bytes[i+2])
			port100.X_13 = v
			i = i + 2

		// 	// case 0x14:
		// 	//     //data.heartbeat = data.remainingData.slice(0,4);
		// 	//     bytes[index++];bytes[index++];
		// 	//     break;

		// 	// case 0x15:
		// 	//     //data.oxigenVolume = data.remainingData.slice(0,4);
		// 	//     bytes[index++];bytes[index++];
		// 	//     break;

		// case 0x16:
		// 	// NEED TO DEBUG
		// 	for j := 0; j < 17; j++ {
		// 		sb.WriteString("data_fft_")
		// 		sb.WriteString(strconv.FormatUint(uint64(j), 10))
		// 		sb.WriteString("=")
		// 		v := uint64(bytes[i+1])
		// 		sb.WriteString(strconv.FormatUint(v, 10))
		// 		sb.WriteString(",")
		// 	}
		// 	i = i + 17

		default:
			// fmt.Print("Data not parsed by decode.LoRaImt imtIotProtocolParser()\n")
			break PL
		}
	}

	p, err := json.Marshal(port100)
	if err != nil {
		fmt.Println(err)
		return "Port100 data parsed wrongly"
	}
	return string(p[:])
}

// CONVERT B64 to BYTE
func b64ToByte(b64 string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		log.Fatal(err)
	}
	return b, err
}

func parseLnsMeasurement(measurement string, data string, port uint64) string {
	// measurements format
	var sb strings.Builder

	if data == "" {
		return "No data"
	}

	// B64 to Byte
	b, err := b64ToByte(data)
	if err != nil {
		// fmt.Print(data)
		log.Panic(err)
	}

	// TODO: SELECT PORT -> DECODE DATA ACCORDING PORT -> SELECT MEASUREMENT -> RETURN STRING
	switch port {
	case 100:
		var port100 Port100
		d := protocolParserPort100(b)
		json.Unmarshal([]byte(d), &port100)

		switch measurement {
		case "SmartLight":
			var smartLight SmartLight
			smartLight.Temperature = port100.X_01
			smartLight.Humidity = port100.X_02
			smartLight.Movement = port100.X_0B
			smartLight.Luminosity = float64(port100.X_0D_0)
			smartLight.BatteryVoltage = float64(port100.X_0D_1)
			smartLight.BoardVoltage = port100.X_0C

			sb.WriteString(`,temperature=`)
			sb.WriteString(strconv.FormatFloat(smartLight.Temperature, 'f', -1, 64))
			sb.WriteString(`,humidity=`)
			sb.WriteString(strconv.FormatFloat(smartLight.Humidity, 'f', -1, 64))
			sb.WriteString(`,movement=`)
			sb.WriteString(strconv.FormatUint(uint64(smartLight.Movement), 10))
			sb.WriteString(`,luminosity=`)
			sb.WriteString(strconv.FormatFloat(smartLight.Luminosity, 'f', -1, 64))
			sb.WriteString(`,batteryVoltage=`)
			sb.WriteString(strconv.FormatFloat(smartLight.BatteryVoltage, 'f', -1, 64))
			sb.WriteString(`,boardVoltage=`)
			sb.WriteString(strconv.FormatFloat(smartLight.BoardVoltage, 'f', -1, 64))

		case "WaterTankLevel":
			var waterTankLevel WaterTankLevel
			waterTankLevel.Distance = port100.X_13
			waterTankLevel.BoardVoltage = port100.X_0C

			sb.WriteString(`,distance=`)
			sb.WriteString(strconv.FormatUint(uint64(waterTankLevel.Distance), 10))
			sb.WriteString(`,boardVoltage=`)
			sb.WriteString(strconv.FormatFloat(waterTankLevel.BoardVoltage, 'f', -1, 64))

		case "GaugePressure":
			var gaugePressure GaugePressure
			gaugePressure.InletPressure = float64(port100.X_0D_0)
			gaugePressure.OutletPressure = float64(port100.X_0D_1)
			gaugePressure.BoardVoltage = port100.X_0C

			sb.WriteString(`,inletPressure=`)
			sb.WriteString(strconv.FormatFloat(gaugePressure.InletPressure, 'f', -1, 64))
			sb.WriteString(`,outletPressure=`)
			sb.WriteString(strconv.FormatFloat(gaugePressure.OutletPressure, 'f', -1, 64))
			sb.WriteString(`,boardVoltage=`)
			sb.WriteString(strconv.FormatFloat(gaugePressure.BoardVoltage, 'f', -1, 64))

		case "Hydrometer":
			var hydrometer Hydrometer
			hydrometer.Counter = port100.X_0B
			hydrometer.BoardVoltage = port100.X_0C

			sb.WriteString(`,counter=`)
			sb.WriteString(strconv.FormatUint(uint64(hydrometer.Counter), 10))
			sb.WriteString(`,boardVoltage=`)
			sb.WriteString(strconv.FormatFloat(hydrometer.BoardVoltage, 'f', -1, 64))

		case "EnergyMeter":
			var energyMeter EnergyMeter
			energyMeter.ForwardEnergy = port100.X_0E_0
			energyMeter.ReverseEnergy = port100.X_0E_1
			energyMeter.BoardVoltage = port100.X_0C

			sb.WriteString(`,forwardEnergy=`)
			sb.WriteString(strconv.FormatFloat(energyMeter.ForwardEnergy, 'f', -1, 64))
			sb.WriteString(`,reverseEnergy=`)
			sb.WriteString(strconv.FormatFloat(energyMeter.ReverseEnergy, 'f', -1, 64))
			sb.WriteString(`,boardVoltage=`)
			sb.WriteString(strconv.FormatFloat(energyMeter.BoardVoltage, 'f', -1, 64))

		default:
		}

	case 4:
		var port4 Port4
		d := protocolParserPort4(b)
		json.Unmarshal([]byte(d), &port4)

		switch measurement {
		case "WeatherStation":
			var weatherStation WeatherStation

			weatherStation.PowerSource = port4.PowerSource
			sb.WriteString(`,powerSource=`)
			sb.WriteString(strconv.FormatBool(weatherStation.PowerSource))

			if port4.IsEnvSensorFailStatus == true {
				weatherStation.EnvSensorFailStatus = port4.EnvSensorFailStatus
				sb.WriteString(`,envSensorFailStatus=`)
				sb.WriteString(strconv.FormatBool(weatherStation.EnvSensorFailStatus))
			}
			if port4.IsInternalBatteryVoltage == true {
				weatherStation.InternalBatteryVoltage = port4.InternalBatteryVoltage
				sb.WriteString(`,internalBatteryVoltage=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.InternalBatteryVoltage, 'f', -1, 64))
			}
			if port4.IsFirmwareVersion == true {
				weatherStation.FirmwareVersion = port4.FirmwareVersion
				sb.WriteString(`,firmwareVersion=`)
				sb.WriteString(strconv.FormatUint(weatherStation.FirmwareVersion, 10))
			}
			if port4.IsC1State == true {
				weatherStation.C1State = port4.C1State
				sb.WriteString(`,c1State=`)
				sb.WriteString(strconv.FormatBool(weatherStation.C1State))
			}
			if port4.IsC1Count == true {
				weatherStation.C1Count = port4.C1Count
				sb.WriteString(`,c1Count=`)
				sb.WriteString(strconv.FormatUint(weatherStation.C1Count, 10))
			}
			if port4.IsC2State == true {
				weatherStation.C2State = port4.C2State
				sb.WriteString(`,c2State=`)
				sb.WriteString(strconv.FormatBool(weatherStation.C2State))
			}
			if port4.IsC2Count == true {
				weatherStation.C2Count = port4.C2Count
				sb.WriteString(`,c2Count=`)
				sb.WriteString(strconv.FormatUint(weatherStation.C2Count, 10))
			}
			if port4.IsInternalTemperature == true {
				weatherStation.InternalTemperature = port4.InternalTemperature
				sb.WriteString(`,internalTemperature=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.InternalTemperature, 'f', -1, 64))
			}
			if port4.IsInternalHumidity == true {
				weatherStation.InternalHumidity = port4.InternalHumidity
				sb.WriteString(`,internalHumidity=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.InternalHumidity, 'f', -1, 64))
			}
			if port4.IsEmwRainLevel == true {
				weatherStation.EmwRainLevel = port4.EmwRainLevel
				sb.WriteString(`,emwRainLevel=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.EmwRainLevel, 'f', -1, 64))
			}
			if port4.IsEmwAvgWindSpeed == true {
				weatherStation.EmwAvgWindSpeed = port4.EmwAvgWindSpeed
				sb.WriteString(`,emwAvgWindSpeed=`)
				sb.WriteString(strconv.FormatUint(weatherStation.EmwAvgWindSpeed, 10))
			}
			if port4.IsEmwGustWindSpeed == true {
				weatherStation.EmwGustWindSpeed = port4.EmwGustWindSpeed
				sb.WriteString(`,emwGustWindSpeed=`)
				sb.WriteString(strconv.FormatUint(weatherStation.EmwGustWindSpeed, 10))
			}
			if port4.IsEmwWindDirection == true {
				weatherStation.EmwWindDirection = port4.EmwWindDirection
				sb.WriteString(`,emwWindDirection=`)
				sb.WriteString(strconv.FormatUint(weatherStation.EmwWindDirection, 10))
			}
			if port4.IsEmwTemperature == true {
				weatherStation.EmwTemperature = port4.EmwTemperature
				sb.WriteString(`,emwTemperature=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.EmwTemperature, 'f', -1, 64))
			}
			if port4.IsEmwHumidity == true {
				weatherStation.EmwHumidity = port4.EmwHumidity
				sb.WriteString(`,emwHumidity=`)
				sb.WriteString(strconv.FormatUint(weatherStation.EmwHumidity, 10))
			}
			if port4.IsEmwLuminosity == true {
				weatherStation.EmwLuminosity = port4.EmwLuminosity
				sb.WriteString(`,emwLuminosity=`)
				sb.WriteString(strconv.FormatUint(weatherStation.EmwLuminosity, 10))
			}
			if port4.IsEmwUv == true {
				weatherStation.EmwUv = port4.EmwUv
				sb.WriteString(`,emwUv=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.EmwUv, 'f', -1, 64))
			}
			if port4.IsEmwSolarRadiation == true {
				weatherStation.EmwSolarRadiation = port4.EmwSolarRadiation
				sb.WriteString(`,emwSolarRadiation=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.EmwSolarRadiation, 'f', -1, 64))
			}
			if port4.IsEmwAtmPres == true {
				weatherStation.EmwAtmPres = port4.EmwAtmPres
				sb.WriteString(`,emwAtmPres=`)
				sb.WriteString(strconv.FormatFloat(weatherStation.EmwAtmPres, 'f', -1, 64))
			}
		default:
		}
	}
	return sb.String()
}

func parseLns(measurement string, deviceId string, direction string, etc string, message string) string {
	var sb strings.Builder
	var lnsUp LnsUp
	var lnsDown LnsDown
	var lnsImtUp LnsImtUp
	// var lnsImtDown LnsImtDown
	var lnsChirpStackV4Up LnsChirpStackV4Up
	// var lnsChirpstackV4Down LnsChirpstackV4Down

	// fmt.Printf("\nmeasurement %s", measurement)
	// fmt.Printf("\ndeviceId %s", deviceId)
	// fmt.Printf("\ndirection %s", direction)
	// fmt.Printf("\netc %s", etc)
	// fmt.Printf("\nmessage %s", message)

	if message == "" {
		// return message, errors.New("empty message to parse")
		// return influx
		return "No message to parse"
	}

	switch etc {
	case "imt":
		if direction == "up" {
			json.Unmarshal([]byte(message), &lnsImtUp)

			lnsUp.Measurement = measurement
			lnsUp.DeviceId = lnsImtUp.DevEUI
			lnsUp.RxInfoMac_0 = lnsImtUp.RxInfo[0].Mac
			lnsUp.RxInfoTime_0 = lnsImtUp.RxInfo[0].Time.Unix() * 1000 * 1000 * 1000
			lnsUp.RxInfoRssi_0 = lnsImtUp.RxInfo[0].Rssi
			lnsUp.RxInfoSnr_0 = lnsImtUp.RxInfo[0].LoRaSNR
			lnsUp.RxInfoLat_0 = lnsImtUp.RxInfo[0].Latitude
			lnsUp.RxInfoLon_0 = lnsImtUp.RxInfo[0].Longitude
			lnsUp.RxInfoAlt_0 = lnsImtUp.RxInfo[0].Altitude
			lnsUp.TxInfoFrequency = lnsImtUp.TxInfo.Frequency / 1000000
			lnsUp.TxInfoModulation = lnsImtUp.TxInfo.DataRate.Modulation
			lnsUp.TxInfoBandWidth = lnsImtUp.TxInfo.DataRate.Bandwidth
			lnsUp.TxInfoSpreadFactor = lnsImtUp.TxInfo.DataRate.SpreadFactor
			// lnsUp.TxInfoCodeRate = lnsImtUp.TxInfo.CodeRate
			lnsUp.FCnt = lnsImtUp.FCnt
			lnsUp.FPort = lnsImtUp.FPort
			lnsUp.FType = "uplink"
			lnsUp.Data = lnsImtUp.Data
		}

	case "chirpstackv4":
		if direction == "up" {
			json.Unmarshal([]byte(message), &lnsChirpStackV4Up)
			// fmt.Printf("\nmessage from chirpstackv4 parseLns %s", message)

			lnsUp.Measurement = measurement
			lnsUp.DeviceId = lnsChirpStackV4Up.DeviceInfo.DevEui
			lnsUp.RxInfoMac_0 = lnsChirpStackV4Up.RxInfo[0].GatewayId
			lnsUp.RxInfoTime_0 = lnsChirpStackV4Up.RxInfo[0].NsTime.UnixNano()
			lnsUp.RxInfoRssi_0 = lnsChirpStackV4Up.RxInfo[0].Rssi
			lnsUp.RxInfoSnr_0 = lnsChirpStackV4Up.RxInfo[0].Snr
			lnsUp.RxInfoLat_0 = lnsChirpStackV4Up.RxInfo[0].Location.Latitude
			lnsUp.RxInfoLon_0 = lnsChirpStackV4Up.RxInfo[0].Location.Longitude
			lnsUp.RxInfoAlt_0 = lnsChirpStackV4Up.RxInfo[0].Location.Altitude
			lnsUp.TxInfoFrequency = lnsChirpStackV4Up.TxInfo.Frequency / 1000000
			lnsUp.TxInfoModulation = "LORA"
			lnsUp.TxInfoBandWidth = lnsChirpStackV4Up.TxInfo.Modulation.Lora.Bandwidth / 1000
			lnsUp.TxInfoSpreadFactor = lnsChirpStackV4Up.TxInfo.Modulation.Lora.SpreadingFactor
			// lnsUp.TxInfoCodeRate = lnsChirpStackV4Up.TxInfo.Modulation.Lora.CodeRate
			lnsUp.FCnt = lnsChirpStackV4Up.FCnt
			lnsUp.FPort = lnsChirpStackV4Up.FPort
			lnsUp.FType = "uplink"
			lnsUp.Data = lnsChirpStackV4Up.Data
			// fmt.Printf("\nlnsUp.Data %s", lnsUp.Data)
		}

	case "atc":
		// lns.Measurement = measurement
		// lns.DeviceId = lnsImt.DevEUI
		// lns.RxInfoMac_0 = lnsImt.RxInfo[0].Mac
		// lns.RxInfoTime_0 = lnsImt.RxInfo[0].Time.Unix() * 1000 * 1000 * 1000
		// lns.RxInfoRssi_0 = lnsImt.RxInfo[0].Rssi
		// lns.RxInfoSnr_0 = lnsImt.RxInfo[0].LoRaSNR
		// lns.RxInfoLat_0 = lnsImt.RxInfo[0].Latitude
		// lns.RxInfoLon_0 = lnsImt.RxInfo[0].Longitude
		// lns.RxInfoAlt_0 = lnsImt.RxInfo[0].Altitude
		// lns.TxInfoFrequency = lnsImt.TxInfo.Frequency / 1000000
		// lns.TxInfoModulation = lnsImt.TxInfo.DataRate.Modulation
		// lns.TxInfoBandWidth = lnsImt.TxInfo.DataRate.Bandwidth
		// lns.TxInfoSpreadFactor = lnsImt.TxInfo.DataRate.SpreadFactor
		// lns.TxInfoCodeRate = lnsImt.TxInfo.CodeRate
		// lns.FCnt = lnsImt.FCnt
		// lns.FPort = lnsImt.FPort
		// lns.FType = "uplink"
		// lns.Data = lnsImt.Data

	default:
	}

	// json.Unmarshal([]byte(message), &lns)

	if direction == "up" {
		// Measurement
		sb.WriteString(lnsUp.Measurement)

		// Tags
		sb.WriteString(`,deviceType=LNS`)
		sb.WriteString(`,deviceId=`)
		sb.WriteString(deviceId)
		sb.WriteString(`,direction=`)
		sb.WriteString(direction)
		sb.WriteString(`,origin=`)
		sb.WriteString(etc)

		sb.WriteString(`,type=`)
		sb.WriteString(lnsUp.FType)
		sb.WriteString(`,rxMac_0=`)
		sb.WriteString(lnsUp.RxInfoMac_0)
		sb.WriteString(`,txModulation=`)
		sb.WriteString(lnsUp.TxInfoModulation)
		// sb.WriteString(`,txCodeRate=`)
		// sb.WriteString(lns.TxInfoCodeRate)

		// Fields
		sb.WriteString(` `)
		sb.WriteString(`txFrequency=`)
		sb.WriteString(strconv.FormatFloat(lnsUp.TxInfoFrequency, 'f', -1, 64))
		sb.WriteString(`,txBandWidth=`)
		sb.WriteString(strconv.FormatUint(uint64(lnsUp.TxInfoBandWidth), 10))
		sb.WriteString(`,txSpreadFactor=`)
		sb.WriteString(strconv.FormatUint(uint64(lnsUp.TxInfoSpreadFactor), 10))
		sb.WriteString(`,rxRssi_0=`)
		sb.WriteString(strconv.FormatInt(int64(lnsUp.RxInfoRssi_0), 10))
		sb.WriteString(`,rxSnr_0=`)
		sb.WriteString(strconv.FormatFloat(lnsUp.RxInfoSnr_0, 'f', -1, 64))
		sb.WriteString(`,rxLat_0=`)
		sb.WriteString(strconv.FormatFloat(lnsUp.RxInfoLat_0, 'f', -1, 64))
		sb.WriteString(`,rxLon_0=`)
		sb.WriteString(strconv.FormatFloat(lnsUp.RxInfoLon_0, 'f', -1, 64))
		sb.WriteString(`,rxAlt_0=`)
		sb.WriteString(strconv.FormatUint(uint64(lnsUp.RxInfoAlt_0), 10))
		sb.WriteString(`,fPort=`)
		sb.WriteString(strconv.FormatUint(uint64(lnsUp.FPort), 10))
		sb.WriteString(`,fCnt=`)
		sb.WriteString(strconv.FormatUint(uint64(lnsUp.FCnt), 10))
		sb.WriteString(`,data="`)
		sb.WriteString(lnsUp.Data)
		sb.WriteString(`"`)

		sb.WriteString(parseLnsMeasurement(lnsUp.Measurement, lnsUp.Data, lnsUp.FPort))

		// Timestamp_ms
		sb.WriteString(` `)
		sb.WriteString(strconv.FormatInt(int64(lnsUp.RxInfoTime_0), 10))
	}
	// fmt.Printf("\n\nChirpstack %s\n\n", sb.String())

	if direction == "down" {
		json.Unmarshal([]byte(message), &lnsDown)

		// Measurement
		sb.WriteString(measurement)

		// Tags
		sb.WriteString(`,deviceType=LNS`)
		sb.WriteString(`,deviceId=`)
		sb.WriteString(deviceId)
		sb.WriteString(`,direction=`)
		sb.WriteString(direction)
		sb.WriteString(`,origin=`)
		sb.WriteString(etc)

		sb.WriteString(`,application=`)
		sb.WriteString(lnsDown.Application)
		sb.WriteString(`,reference=`)
		sb.WriteString(lnsDown.Reference)

		// Fields
		sb.WriteString(` `)
		sb.WriteString(`confirmed=`)
		sb.WriteString(strconv.FormatBool(lnsDown.Confirmed))
		sb.WriteString(`,fPort=`)
		sb.WriteString(strconv.FormatUint(uint64(lnsDown.FPort), 10))
		sb.WriteString(`,data="`)
		sb.WriteString(lnsDown.Data)
		sb.WriteString(`"`)

		// Timestamp_ms
		sb.WriteString(` `)
		sb.WriteString(strconv.FormatInt(int64(lnsDown.Timestamp), 10))
	}
	return sb.String()
}

// func parseEvse(deviceId string, direction string, etc string, message string) string {
// 	var s string
//		return s
//	}

func connLostHandler(c MQTT.Client, err error) {
	fmt.Printf("Connection lost, reason: %v\n", err)
	os.Exit(1)
}

func main() {
	id := uuid.New().String()
	ORGANIZATION := os.Getenv("ORGANIZATION")
	DEVICE_TYPE := os.Getenv("DEVICE_TYPE")
	BUCKET := os.Getenv("BUCKET")
	MQTT_BROKER := os.Getenv("MQTT_BROKER")
	kafkaBroker := os.Getenv("KAFKA_BROKER")

	// MqttSubscriberClient
	var sbMqttSubClientId strings.Builder
	sbMqttSubClientId.WriteString("parse-lns-sub-")
	sbMqttSubClientId.WriteString(id)

	// MqttSubscriberTopic
	var sbMqttSubTopic strings.Builder
	// sbMqttSubTopic.WriteString("debug/OpenDataTelemetry/")
	sbMqttSubTopic.WriteString("OpenDataTelemetry/")
	sbMqttSubTopic.WriteString(ORGANIZATION)
	sbMqttSubTopic.WriteString("/")
	sbMqttSubTopic.WriteString(DEVICE_TYPE)
	sbMqttSubTopic.WriteString("/+/+/+/+")
	// sbMqttSubTopic.WriteString("/+/+/+")

	// KafkaProducerClient
	var sbKafkaProdClientId strings.Builder
	sbKafkaProdClientId.WriteString("parse-lns-prod-")
	sbKafkaProdClientId.WriteString(id)

	// KafkaProducerClient
	var sbKafkaProdTopic strings.Builder
	sbKafkaProdTopic.WriteString(ORGANIZATION)
	sbKafkaProdTopic.WriteString(".")
	sbKafkaProdTopic.WriteString(BUCKET)

	// MQTT
	mqttSubBroker := MQTT_BROKER
	mqttSubClientId := sbMqttSubClientId.String()
	mqttSubUser := "public"
	mqttSubPassword := "public"
	mqttSubQos := 0

	mqttSubOpts := MQTT.NewClientOptions()
	mqttSubOpts.AddBroker(mqttSubBroker)
	mqttSubOpts.SetClientID(mqttSubClientId)
	mqttSubOpts.SetUsername(mqttSubUser)
	mqttSubOpts.SetPassword(mqttSubPassword)
	mqttSubOpts.SetConnectionLostHandler(connLostHandler)

	c := make(chan [2]string)

	mqttSubOpts.SetDefaultPublishHandler(func(mqttClient MQTT.Client, msg MQTT.Message) {
		c <- [2]string{msg.Topic(), string(msg.Payload())}
	})

	mqttSubClient := MQTT.NewClient(mqttSubOpts)
	if token := mqttSubClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Printf("Connected to %s\n", mqttSubBroker)
	}

	if token := mqttSubClient.Subscribe(sbMqttSubTopic.String(), byte(mqttSubQos), nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	// KAFKA
	// kafkaProdClient, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "my-cluster-kafka-bootstrap.test-kafka.svc.cluster.local"})
	kafkaProdClient, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	if err != nil {
		panic(err)
	}
	defer kafkaProdClient.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range kafkaProdClient.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("\nDelivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// MQTT -> KAFKA
	for {
		// 1. Input
		incoming := <-c

		// 2. Process
		// 2.1. Process Topic
		s := strings.Split(incoming[0], "/")
		// OpenDataTelemetry/IMT/LNS/MEASUREMENT/DEVICE_ID/up/imt
		// OpenDataTelemetry/IMT/LNS/MEASUREMENT/DEVICE_ID/down/chirpstackv4
		measurement := s[3]
		deviceId := s[4]
		direction := s[5]
		etc := s[6]

		// // DEBUG
		// measurement := s[4]
		// deviceId := s[5]
		// direction := s[6]
		// etc := s[7]

		var kafkaMessage string

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

		if ORGANIZATION == "IMT" {

			switch DEVICE_TYPE {
			case "LNS":
				// var influx Influx
				kafkaMessage = parseLns(measurement, deviceId, direction, etc, incoming[1])
				fmt.Printf("\nMessage: %s", kafkaMessage)

			case "EVSE":
				// kafkaMessage = parseEvse(deviceId, direction, etc, incoming[1])

			default:
			}
		}

		// return influx line protocol
		// measurement,tags fields timestamp
		// fmt.Printf("InfluxLineProtocol: %s\n", kafkaMessage)

		// SET KAFKA
		kafkaProdTopic := sbKafkaProdTopic.String()
		// pClient.Publish(sbPubTopic.String(), byte(pQos), false, incoming[1])

		kafkaProdClient.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaProdTopic, Partition: kafka.PartitionAny},
			Value:          []byte(kafkaMessage),
			Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		}, nil)
		if err != nil {
			fmt.Printf("Produce failed: %v\n", err)
			os.Exit(1)
		}

		kafkaProdClient.Flush(15 * 1000)
	}
}
