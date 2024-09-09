package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
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
type Lns struct {
	Measurement        string  `json:"measurement"`
	DeviceId           string  `json:"LNS"`
	RxInfoMac_0        string  `json:"rxInfo_mac_0"`
	RxInfoTime_0       int64   `json:"rxInfo_time_0"`
	RxInfoRssi_0       int64   `json:"rxInfo_rssi_0"`
	RxInfoSnr_0        float64 `json:"rxInfo_snr_0"`
	RxInfoLat_0        float64 `json:"rxInfo_lat_0"`
	RxInfoLon_0        float64 `json:"rxInfo_lon_0"`
	RxInfoAlt_0        uint64  `json:"rxInfo_alt_0"`
	TxInfoFrequency    float64 `json:"txInfo_frequency"`
	TxInfoModulation   string  `json:"txInfo_modulation"`
	TxInfoBandWidth    uint64  `json:"txInfo_bandwidth"`
	TxInfoSpreadFactor uint64  `json:"txInfo_spreadFactor"`
	TxInfoCodeRate     string  `json:"txInfo_codeRate"`
	FCnt               uint64  `json:"fCnt"`
	FPort              uint64  `json:"fPort"`
	FType              string  `json:"fType"`
	Data               string  `json:"data"`
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
	InternalTemperature float64 `json:"internalTemperature"`
	InternalHumidity    float64 `json:"internalHumidity"`
	RainGauge           float64 `json:"rainLevel"`
	AvgWindSpeed        float64 `json:"avgWindSpeed"`
	GustWindSpeed       float64 `json:"gustWindSpeed"`
	WindDirection       string  `json:"windDirection"`
	Temperature         float64 `json:"temperature"`
	Humidity            float64 `json:"humidity"`
	Luminosity          float64 `json:"luminosity"`
	Uv                  float64 `json:"uv"`
	SolarRadiation      float64 `json:"solarRadiation"`
}

// Wind direction
// Wind velocity
// Rain gauge
// Temperature
// Humidity
// UV Radiation
// Atmospheric pressure

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

type LnsChirpStackV4 struct {
	DeduplicationId string                    `json:"deduplicationId"`
	DeviceInfo      LnsChirpStackV4DeviceInfo `json:"deviceInfo"`
	DevAddr         string                    `json:"devAddr"`
	Adr             bool                      `json:"adr"`
	Dr              uint64                    `json:"dr"`
	FCnt            uint64                    `json:"fCnt"`
	FPort           uint64                    `json:"fPort"`
	Confirmed       string                    `json:"Confirmed"`
	RxInfo          []LnsChirpStackV4RxInfo   `json:"rxInfo"`
	TxInfo          LnsChirpStackV4TxInfo     `json:"txInfo"`
	Data            string                    `json:"data"`
}

type LnsChirpStackV4DeviceInfo struct {
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

type LnsChirpStackV4RxInfo struct {
	GatewayId         string                  `json:"gatewayId"`
	UplinkId          uint64                  `json:"uplinkId"`
	NsTime            time.Time               `json:"nsTime"`
	TimeSinceGpsEpoch string                  `json:"timeSinceGpsEpoch"`
	Rssi              int64                   `json:"rssi"`
	Snr               float64                 `json:"snr"`
	Channel           uint64                  `json:"channel"`
	Board             uint64                  `json:"board"`
	Location          LnsChirpStackV4Location `json:"location"`
	Context           string                  `json:"context"`
	Metadata          LnsChirpStackV4Metadata `json:"metadata"`
	CrcStatus         string                  `json:"crcStatus"`
}

type LnsChirpStackV4Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Altitude  uint64  `json:"altitude"`
}

type LnsChirpStackV4Metadata struct {
	Region_config_id   string `json:"region_config_id"`
	Region_common_name string `json:"region_common_name"`
}

type LnsChirpStackV4TxInfo struct {
	Frequency  float64                   `json:"frequency"`
	Modulation LnsChirpStackV4Modulation `json:"modulation"`
}

type LnsChirpStackV4Modulation struct {
	Lora LnsChirpStackV4Lora `json:"lora"`
}

type LnsChirpStackV4Lora struct {
	Bandwidth       uint64 `json:"bandwidth"`
	SpreadingFactor uint64 `json:"spreadingFactor"`
	CodeRate        string `json:"codeRate"`
}

type LnsImt struct {
	ApplicationID   string         `json:"applicationID"`
	ApplicationName string         `json:"applicationName"`
	NodeName        string         `json:"nodeName"`
	DevEUI          string         `json:"devEUI"`
	RxInfo          []LnsImtRxInfo `json:"rxInfo"`
	TxInfo          LnsImtTxInfo   `json:"txInfo"`
	FCnt            uint64         `json:"fCnt"`
	FPort           uint64         `json:"FPort"`
	Data            string         `json:"data"`
}

type LnsImtRxInfo struct {
	Mac       string    `json:"mac"`
	Time      time.Time `json:"time"`
	Rssi      int64     `json:"rssi"`
	LoRaSNR   float64   `json:"loRaSNR"`
	Name      string    `json:"name"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Altitude  uint64    `json:"altitude"`
}

type LnsImtDataRate struct {
	Modulation   string `json:"modulation"`
	Bandwidth    uint64 `json:"bandwidth"`
	SpreadFactor uint64 `json:"spreadFactor"`
}

type LnsImtTxInfo struct {
	Frequency float64        `json:"frequency"`
	DataRate  LnsImtDataRate `json:"dataRate"`
	Adr       bool           `json:"adr"`
	CodeRate  string         `json:"codeRate"`
}

// func protocolParserPort4(bytes []byte) string {
// 	// 	var port4 Port4
//
// 	// 	len := len(bytes)
//
// 	// 	//   var decoded = {};
// 	// 	//   var index = 0;
// 	// 	//   var mask_sensor_int = bytes[index++];
// 	// 	//   var mask_sensor_ext = bytes[index++];
// 	// 	//   var status_dry = ["OPEN", "CLOSED"];
// 	// 	//   var status_relay = ["NO", "NC"];
// 	// 	//   var novo_firmware = 0;
//
// 	// 	//  if (bytes.length > index)
// 	// 	//  {
// 	// 	//       if (mask_sensor_int & 0x19)
// 	// 	//       {
// 	// 	//           decoded.internal_sensors = [];
// 	// 	//       }

// 	// 	//       if(mask_sensor_int & 0x80)
// 	// 	//       {
// 	// 	//           decoded.internal_sensors = [];
// 	// 	//           novo_firmware = 1;
// 	// 	//           mask_sensor_ext = bytes[index++]; // aumenta mais 1 index por ser novo firmware
// 	// 	//       }

// 	// 	//       // Decode Battery
// 	// 	//       if (mask_sensor_int >> 0 & 0x01)
// 	// 	//       {
// 	// 	//           var battery = {};
// 	// 	//           battery.n = 'battery';
// 	// 	//           battery.v = (bytes[index++] / 10.0).toFixed(1);
// 	// 	//           battery.u = 'V';
// 	// 	//           decoded.internal_sensors.push(battery);
// 	// 	//       }

// 	// 	//       // Decode Firmware Version
// 	// 	//       if (mask_sensor_int >> 2 & 0x01) {
// 	// 	//           var firmware_version = {n : "version"};
// 	// 	//           var firmware = bytes[index++] | (bytes[index++] << 8) | (bytes[index++] << 16);
// 	// 	//           var hardware = (firmware / 1000000) >>> 0;
// 	// 	//           var compatibility = ((firmware / 10000) - (hardware * 100)) >>> 0;
// 	// 	//           var feature = ((firmware - (hardware * 1000000) - (compatibility * 10000)) / 100) >>> 0;
// 	// 	//           var bug = (firmware - (hardware * 1000000) - (compatibility * 10000) - (feature * 100)) >>> 0;

// 	// 	//           firmware_version.v = hardware + '.' + compatibility + '.' + feature + '.' + bug;
// 	// 	//           decoded.device.push(firmware_version);
// 	// 	//       }

// 	// 	//       // Decode Temperature Int
// 	// 	//       if (mask_sensor_int >> 3 & 0x01) { //3
// 	// 	//           var temperature = {};
// 	// 	//           temperature.v = bytes[index++] | (bytes[index++] << 8);
// 	// 	//           temperature.v = ((temperature.v / 100.0) - 273.15).toFixed(2);
// 	// 	//           temperature.n = "Int. temp.";
// 	// 	//           temperature.u = "C";

// 	// 	//           decoded.internal_sensors.push(temperature);
// 	// 	//       }

// 	// 	//       // Decode Moisture Int
// 	// 	//       if (mask_sensor_int >> 4 & 0x01) {   //4
// 	// 	//           var humidity = {};
// 	// 	//           humidity.v = bytes[index++] | (bytes[index++] << 8);
// 	// 	//           humidity.v = (humidity.v / 10.0).toFixed(2);
// 	// 	//           humidity.n = "Int. hum.";
// 	// 	//           humidity.u = "%";
// 	// 	//           decoded.internal_sensors.push(humidity);
// 	// 	//       }

// 	// 	//       var dry = {};
// 	// 	//       if (novo_firmware == 1)
// 	// 	//       {// Decode Drys
// 	// 	//           if (mask_sensor_ext & 0x0F) {
// 	// 	//               decoded.drys = [];

// 	// 	//               // Decode Dry 1 State
// 	// 	//               if (mask_sensor_ext >> 0 & 0x01) {
// 	// 	//                   dry = {};
// 	// 	//                   dry.n = 'C1 State';
// 	// 	//                   dry.v = status_dry[(mask_sensor_ext >> 0 & 0x01)];
// 	// 	//                   decoded.drys.push(dry);
// 	// 	//               }

// 	// 	//               // // Decode Dry 1 Count
// 	// 	//               // if (mask_sensor_ext >> 1 & 0x01) {
// 	// 	//               //      dry = {};
// 	// 	//               //     dry.n = 'C1 Count';
// 	// 	//               //     dry.v = bytes[index++] | (bytes[index++] << 8);
// 	// 	//               //     decoded.drys.push(dry);
// 	// 	//               // }

// 	// 	//               // Decode Dry 2 State
// 	// 	//               if (mask_sensor_ext >> 2 & 0x01) {
// 	// 	//                    dry = {};
// 	// 	//                   dry.n = 'C2 State';
// 	// 	//                   dry.v = status_dry[(mask_sensor_ext >> 2 & 0x01)];
// 	// 	//                   decoded.drys.push(dry);
// 	// 	//               }

// 	// 	//               // // Decode Dry 2 Count
// 	// 	//               // if (mask_sensor_ext >> 3 & 0x01) {
// 	// 	//               //      dry = {};
// 	// 	//               //     dry.n = 'C2 Count';
// 	// 	//               //     dry.v = bytes[index++] | (bytes[index++] << 8);
// 	// 	//               //     decoded.drys.push(dry);
// 	// 	//               // }
// 	// 	//           }
// 	// 	//           index++; // acerta contagem
// 	// 	//           index++;
// 	// 	//       }
// 	// 	//       else
// 	// 	//       {// Decode Drys
// 	// 	//           if (mask_sensor_ext & 0x0F) {
// 	// 	//               decoded.drys = [];

// 	// 	//               // Decode Dry 1 State
// 	// 	//               if (mask_sensor_ext >> 0 & 0x01) {
// 	// 	//                   dry = {};
// 	// 	//                   dry.n = 'C1 State';
// 	// 	//                   dry.v = status_dry[bytes[index++]];
// 	// 	//                   decoded.drys.push(dry);
// 	// 	//               }

// 	// 	//               // Decode Dry 1 Count
// 	// 	//               if (mask_sensor_ext >> 1 & 0x01) {
// 	// 	//                    dry = {};
// 	// 	//                   dry.n = 'C1 Count';
// 	// 	//                   dry.v = bytes[index++] | (bytes[index++] << 8);
// 	// 	//                   decoded.drys.push(dry);
// 	// 	//               }

// 	// 	//               // Decode Dry 2 State
// 	// 	//               if (mask_sensor_ext >> 2 & 0x01) {
// 	// 	//                    dry = {};
// 	// 	//                   dry.n = 'C2 State';
// 	// 	//                   dry.v = status_dry[bytes[index++]];
// 	// 	//                   decoded.drys.push(dry);
// 	// 	//               }

// 	// 	//               // Decode Dry 2 Count
// 	// 	//               if (mask_sensor_ext >> 3 & 0x01) {
// 	// 	//                    dry = {};
// 	// 	//                   dry.n = 'C2 Count';
// 	// 	//                   dry.v = bytes[index++] | (bytes[index++] << 8);
// 	// 	//                   decoded.drys.push(dry);
// 	// 	//               }
// 	// 	//           }

// 	// 	//           // Decode DS18B20 Probe
// 	// 	//           if (mask_sensor_ext >> 4 & 0x07) {
// 	// 	//               var nb_probes = (mask_sensor_ext >> 4 & 0x07) >>> 0;

// 	// 	//               decoded.probes = [];

// 	// 	//               for (var i = 0; i < nb_probes; i++) {
// 	// 	//                   var probe = {};

// 	// 	//                   probe.n = 'temperature';
// 	// 	//                   probe.v = (((bytes[index++] | (bytes[index++] << 8)) / 100.0) - 273).toFixed(2);
// 	// 	//                   probe.u = 'C';

// 	// 	//                   if (mask_sensor_ext >> 7 & 0x01) {
// 	// 	//                       index += 7;
// 	// 	//                       probe.rom = (bytes[index--]).toString(16);

// 	// 	//                       for (var j = 0; j < 7; j++) {
// 	// 	//                           probe.rom += (bytes[index--]).toString(16);
// 	// 	//                       }
// 	// 	//                       index += 9;
// 	// 	//                   } else {
// 	// 	//                       probe.rom = bytes[index++];
// 	// 	//                   }
// 	// 	//                   probe.rom = probe.rom.toUpperCase();
// 	// 	//                   decoded.probes.push(probe);
// 	// 	//               }
// 	// 	//           }
// 	// 	//       }

// 	// 	//       // Decode Extension Module(s).
// 	// 	//       if (bytes.length > index)
// 	// 	//       {
// 	// 	//           decoded.modules = [];

// 	// 	//           while (bytes.length > index)
// 	// 	//           {
// 	// 	//               var module_type = {n : "module"};
// 	// 	switch bytes[index] {
// 	// 	//               switch (bytes[index])
// 	// 	//               {
// 	// 	case 1:
// 	// 		//                       {
// 	// 		//                           module_type.v = "EM S104";
// 	// 		//                           index++;
// 	// 		//                           var mask_ems104 = bytes[index++];

// 	// 		//                           // E1
// 	// 		//                           if (mask_ems104 >> 0 & 0x01) {
// 	// 		//                               var conn = {};
// 	// 		//                               conn.n = 'e1_temp';
// 	// 		//                               conn.v = (bytes[index++] | (bytes[index++] << 8));
// 	// 		//                               conn.v = ((conn.v / 100.0) - 273.15).toFixed(2);
// 	// 		//                               conn.u = 'C';
// 	// 		//                               decoded.modules.push(conn);
// 	// 		//                           }

// 	// 		//                           // E2
// 	// 		//                           if (mask_ems104 >> 1 & 0x01) {
// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'e2_kpa';
// 	// 		//                               conn.v = ((bytes[index++] | (bytes[index++] << 8)) / 100.0).toFixed(2);
// 	// 		//                               conn.u = 'kPa';
// 	// 		//                               decoded.modules.push(conn);
// 	// 		//                           }

// 	// 		//                           // E3
// 	// 		//                           if (mask_ems104 >> 2 & 0x01) {
// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'e3_kpa';
// 	// 		//                               conn.v = ((bytes[index++] | (bytes[index++] << 8)) / 100.0).toFixed(2);
// 	// 		//                               conn.u = 'kPa';
// 	// 		//                               decoded.modules.push(conn);
// 	// 		//                           }

// 	// 		//                           // E4
// 	// 		//                           if (mask_ems104 >> 3 & 0x01) {
// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'e4_kpa';
// 	// 		//                               conn.v = ((bytes[index++] | (bytes[index++] << 8)) / 100.0).toFixed(2);
// 	// 		//                               conn.u = 'kPa';
// 	// 		//                               decoded.modules.push(conn);
// 	// 		//                           }
// 	// 		//                       }
// 	// 		//                       break;

// 	// 	case 2:
// 	// 		//                       {
// 	// 		//                           module_type.v = "EM C104";
// 	// 		//                           index++;
// 	// 		//                           var mask_emc104 = bytes[index++];

// 	// 		//                           // Plus (Min Max and Avg)
// 	// 		//                           if (mask_emc104 >> 4 & 0x01) {
// 	// 		//                               for (var k = 0; k < 4; k++) {
// 	// 		//                                   if ((mask_emc104 >> k) & 0x01) {
// 	// 		//                                        conn = {};
// 	// 		//                                       conn.n = 'e' + (k + 1) + '_curr';
// 	// 		//                                       conn.u = "mA";
// 	// 		//                                       // Min
// 	// 		//                                       if (mask_emc104 >> 5 & 0x01) {
// 	// 		//                                           conn.min = (bytes[index++] / 12.0).toFixed(2);
// 	// 		//                                       }
// 	// 		//                                       // Max
// 	// 		//                                       if (mask_emc104 >> 6 & 0x01) {
// 	// 		//                                           conn.max = (bytes[index++] / 12.0).toFixed(2);
// 	// 		//                                       }
// 	// 		//                                       // Avg
// 	// 		//                                       if (mask_emc104 >> 7 & 0x01) {
// 	// 		//                                           conn.avg = (bytes[index++] / 12.0).toFixed(2);
// 	// 		//                                       }
// 	// 		//                                       decoded.modules.push(conn);
// 	// 		//                                   }
// 	// 		//                               }
// 	// 		//                           } else {
// 	// 		//                               // E1
// 	// 		//                               if (mask_emc104 >> 0 & 0x01) {
// 	// 		//                                    conn = {};
// 	// 		//                                   conn.n = 'e1_curr';
// 	// 		//                                   conn.v = ((bytes[index++] | (bytes[index++] << 8)) / 1000).toFixed(2);
// 	// 		//                                   conn.u = "mA";
// 	// 		//                                   decoded.modules.push(conn);
// 	// 		//                               }

// 	// 		//                               // E2
// 	// 		//                               if (mask_emc104 >> 1 & 0x01) {
// 	// 		//                                    conn = {};
// 	// 		//                                   conn.n = 'e2_curr';
// 	// 		//                                   conn.v = ((bytes[index++] | (bytes[index++] << 8)) / 1000).toFixed(2);
// 	// 		//                                   conn.u = "mA";
// 	// 		//                                   decoded.modules.push(conn);
// 	// 		//                               }

// 	// 		//                               // E3
// 	// 		//                               if (mask_emc104 >> 2 & 0x01) {
// 	// 		//                                    conn = {};
// 	// 		//                                   conn.n = 'e3_curr';
// 	// 		//                                   conn.v = ((bytes[index++] | (bytes[index++] << 8)) / 1000).toFixed(2);
// 	// 		//                                   conn.u = "mA";
// 	// 		//                                   decoded.modules.push(conn);
// 	// 		//                               }

// 	// 		//                               // E4
// 	// 		//                               if (mask_emc104 >> 3 & 0x01) {
// 	// 		//                                    conn = {};
// 	// 		//                                   conn.n = 'e4_curr';
// 	// 		//                                   conn.v = ((bytes[index++] | (bytes[index++] << 8)) / 1000).toFixed(2);
// 	// 		//                                   conn.u = "mA";
// 	// 		//                                   decoded.modules.push(conn);
// 	// 		//                               }
// 	// 		//                           }

// 	// 		//                       }
// 	// 		//                       break;

// 	// 		//                   // EM W104
// 	// 	case 4:
// 	// 		//                       {
// 	// 		//                           module_type.v = "EM W104";
// 	// 		//                           index++;
// 	// 		//                           var mask_emw104 = bytes[index++];

// 	// 		//                           //Weather Station
// 	// 		//                           if (mask_emw104 >> 0 & 0x01) {
// 	// 		//                               //Rain
// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'rain_lvl';
// 	// 		//                               conn.v = (((bytes[index++] << 8) | bytes[index++]) / 10.0).toFixed(1);
// 	// 		//                               conn.u = 'mm';
// 	// 		//                               decoded.modules.push(conn);

// 	// 		//                               //Average Wind Speed
// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'avg_wind_speed'
// 	// 		//                               conn.v = bytes[index++].toFixed(0);
// 	// 		//                               conn.u = 'km/h';
// 	// 		//                               decoded.modules.push(conn);

// 	// 		//                               //Gust Wind Speed
// 	// 		//                               conn = {};
// 	// 		//                               conn.n = 'gust_wind_speed';
// 	// 		//                               conn.v = bytes[index++].toFixed(0);
// 	// 		//                               conn.u = 'km/h';
// 	// 		//                               decoded.modules.push(conn);

// 	// 		//                               //Wind Direction
// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'wind_direction';
// 	// 		//                               conn.v = ((bytes[index++] << 8) | bytes[index++]).toFixed(0);
// 	// 		//                               conn.u = 'graus';
// 	// 		//                               decoded.modules.push(conn);

// 	// 		//                               //Temperature
// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'temperature';
// 	// 		//                               conn.v = ((bytes[index++] << 8) | bytes[index++]) / 10.0;
// 	// 		//                               conn.v = (conn.v - 273.15).toFixed(1);
// 	// 		//                               conn.u = 'C';
// 	// 		//                               decoded.modules.push(conn);

// 	// 		//                               //Humidity
// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'humidity';
// 	// 		//                               conn.v = bytes[index++].toFixed(0);
// 	// 		//                               conn.u = '%';
// 	// 		//                               decoded.modules.push(conn);

// 	// 		//                               //Lux and UV
// 	// 		//                               if (mask_emw104 >> 1 & 0x01) {
// 	// 		//                                    conn = {};
// 	// 		//                                   conn.n = 'luminosity';
// 	// 		//                                   conn.v = (bytes[index++] << 16) | (bytes[index++] << 8) | bytes[index++];
// 	// 		//                                   conn.u = 'lx';
// 	// 		//                                   decoded.modules.push(conn);

// 	// 		//                                    conn = {};
// 	// 		//                                   conn.n = 'uv';
// 	// 		//                                   conn.v = bytes[index++];
// 	// 		//                                   conn.v = (conn.v / 10.0).toFixed(1);
// 	// 		//                                   conn.u = '/';
// 	// 		//                                   decoded.modules.push(conn);
// 	// 		//                               }
// 	// 		//                           }

// 	// 		//                           //Pyranometer
// 	// 		//                           if (mask_emw104 >> 2 & 0x01) {
// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'solar_radiation';
// 	// 		//                               conn.v = (bytes[index++] << 8) | bytes[index++];
// 	// 		//                               conn.v = (conn.v / 10.0).toFixed(1);
// 	// 		//                               conn.u = 'W/m²';
// 	// 		//                               decoded.modules.push(conn);
// 	// 		//                           }

// 	// 		//                           //Barometer
// 	// 		//                           if (mask_emw104 >> 3 & 0x01) {
// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'atm_pres';
// 	// 		//                               conn.v = (bytes[index++] << 16);
// 	// 		//                               conn.v |= (bytes[index++] << 8) | bytes[index++] << 0;
// 	// 		//                               conn.v = (conn.v / 100.0).toFixed(1);
// 	// 		//                               conn.u = 'hPa²';
// 	// 		//                               decoded.modules.push(conn);
// 	// 		//                           }
// 	// 		//                       }
// 	// 		//                       break;

// 	// 		//                   // EM R102
// 	// 	case 5:
// 	// 		//                       {
// 	// 		//                           index++;
// 	// 		//                           module_type.v = "EM R102";

// 	// 		//                           var mask_emr102 = bytes[index++];
// 	// 		//                           var mask_data = bytes[index++];

// 	// 		//                           // E1
// 	// 		//                           if (mask_emr102 >> 0 & 0x01) {
// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'C3 Status';
// 	// 		//                               conn.v = status_dry[(mask_data >> 0 & 0x01)];
// 	// 		//                               conn.u = "bool";
// 	// 		//                               decoded.modules.push(conn);

// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'C3 Count';
// 	// 		//                               conn.v = bytes[index++] | (bytes[index++] << 8);
// 	// 		//                               decoded.modules.push(conn);
// 	// 		//                           }

// 	// 		//                           // E2
// 	// 		//                           if (mask_emr102 >> 1 & 0x01) {
// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'C4 Status';
// 	// 		//                               conn.v = status_dry[(mask_data >> 1 & 0x01)];
// 	// 		//                               conn.u = "bool";
// 	// 		//                               decoded.modules.push(conn);

// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'C4 Count';
// 	// 		//                               conn.v = bytes[index++] | (bytes[index++] << 8);
// 	// 		//                               decoded.modules.push(conn);
// 	// 		//                           }

// 	// 		//                           // E3
// 	// 		//                           if (mask_emr102 >> 2 & 0x01) {
// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'B3 Relay';
// 	// 		//                               conn.v = status_relay[(mask_data >> 2 & 0x01)];
// 	// 		//                               decoded.modules.push(conn);
// 	// 		//                           }

// 	// 		//                           // E4
// 	// 		//                           if (mask_emr102 >> 3 & 0x01) {
// 	// 		//                                conn = {};
// 	// 		//                               conn.n = 'B4 Relay';
// 	// 		//                               conn.v = status_relay[(mask_data >> 3 & 0x01)];
// 	// 		//                               decoded.modules.push(conn);
// 	// 		//                           }

// 	// 		//                       }
// 	// 		//                       break;

// 	// 		//                   // EM ACW100 & EM THW 100/200/201
// 	// 	case 6:
// 	// 		//                       {
// 	// 		//                           index++;

// 	// 		//                           var rom = {};
// 	// 		//                           var one_wire_ext_model = 0x00;
// 	// 		//                           var mask_em_acw_thw = bytes[index++];

// 	// 		//                           if (mask_em_acw_thw == 0x03) {
// 	// 		//                               one_wire_ext_model = 0x06;
// 	// 		//                           }
// 	// 		//                           else {
// 	// 		//                               if (mask_em_acw_thw >> 0 & 0x01) {
// 	// 		//                                   one_wire_ext_model |= 0x01;
// 	// 		//                               }

// 	// 		//                               if (mask_em_acw_thw >> 4 & 0x01) {
// 	// 		//                                   one_wire_ext_model |= 0x02;
// 	// 		//                               }
// 	// 		//                           }

// 	// 		//                           switch (one_wire_ext_model) {
// 	// 		//                               case 0x01:
// 	// 		//                                   module_type.v = "EM THW 200";
// 	// 		//                                   break;
// 	// 		//                               case 0x02:
// 	// 		//                                   module_type.v = "EM ACW 100";
// 	// 		//                                   break;
// 	// 		//                               case 0x03:
// 	// 		//                                   module_type.v = "EM THW 201";
// 	// 		//                                   break;
// 	// 		//                               case 0x06:
// 	// 		//                                   module_type.v = "EM THW 100";
// 	// 		//                                   break;
// 	// 		//                               default:
// 	// 		//                                   module_type.v = "Unknow";
// 	// 		//                                   break;
// 	// 		//                           }
// 	// 		//                           decoded.modules.push(module_type);
// 	// 		//                           //ROM
// 	// 		//                           if ((mask_sensor_ext >> 4 & 0x07) && (mask_sensor_ext >> 7 & 0x00)) {
// 	// 		//                               rom.v = bytes[index++];
// 	// 		//                           } else {
// 	// 		//                               index += 7;
// 	// 		//                               rom.v = (bytes[index--]).toString(16);

// 	// 		//                               for ( j = 0; j < 7; j++) {
// 	// 		//                                   rom.v += (bytes[index--]).toString(16);
// 	// 		//                               }
// 	// 		//                               index += 9;
// 	// 		//                           }

// 	// 		//                           rom.v = rom.v.toUpperCase();
// 	// 		//                           rom.n = 'ROM';
// 	// 		//                           decoded.modules.push(rom);

// 	// 		//                           //Temperature
// 	// 		//                           if (mask_em_acw_thw >> 0 & 0x01) {
// 	// 		//                               var sensor = {};
// 	// 		//                               sensor.n = 'temperature';
// 	// 		//                               sensor.u = 'C';
// 	// 		//                               sensor.v = ((bytes[index++] | (bytes[index++] << 8)) / 100.0) - 273.15;
// 	// 		//                               sensor.v = sensor.v.toFixed(2);
// 	// 		//                               decoded.modules.push(sensor);
// 	// 		//                           }

// 	// 		//                           //Humidity
// 	// 		//                           if (mask_em_acw_thw >> 1 & 0x01) {
// 	// 		//                                sensor = {};
// 	// 		//                               sensor.n = 'humidity';
// 	// 		//                               sensor.u = '%';
// 	// 		//                               sensor.v = (bytes[index++] | (bytes[index++] << 8)) / 100.0;
// 	// 		//                               sensor.v = sensor.v.toFixed(2);
// 	// 		//                               decoded.modules.push(sensor);
// 	// 		//                           }

// 	// 		//                           //Lux
// 	// 		//                           if (mask_em_acw_thw >> 2 & 0x01) {
// 	// 		//                                sensor = {};
// 	// 		//                               sensor.n = 'luminosity';
// 	// 		//                               sensor.u = 'lux';
// 	// 		//                               sensor.v = bytes[index++] | (bytes[index++] << 8);
// 	// 		//                               sensor.v = sensor.v.toFixed(2);
// 	// 		//                               decoded.modules.push(sensor);
// 	// 		//                           }

// 	// 		//                           //Noise
// 	// 		//                           if (mask_em_acw_thw >> 3 & 0x01) {
// 	// 		//                                sensor = {};
// 	// 		//                               sensor.n = 'noise';
// 	// 		//                               sensor.u = 'dB';
// 	// 		//                               sensor.v = (bytes[index++] | (bytes[index++] << 8)) / 100.0;
// 	// 		//                               sensor.v = sensor.v.toFixed(2);
// 	// 		//                               decoded.modules.push(sensor);
// 	// 		//                           }

// 	// 		//                           //Temperature RTDT
// 	// 		//                           if (mask_em_acw_thw >> 4 & 0x01) {
// 	// 		//                                sensor = {};
// 	// 		//                               sensor.n = 'temperature_rtdt';
// 	// 		//                               sensor.u = 'C';
// 	// 		//                               sensor.v = bytes[index++];
// 	// 		//                               for ( j = 1; j < 4; j++) {
// 	// 		//                                   sensor.v |= (bytes[index++] << (8 * j));
// 	// 		//                               }
// 	// 		//                               sensor.v = ((sensor.v / 100.0) - 273.15).toFixed(2);
// 	// 		//                               decoded.modules.push(sensor);
// 	// 		//                           }
// 	// 		//                       }
// 	// 		//                       break;

// 	// 	default:
// 	// 	}
// 	// 	//           }
// 	// 	//       }
// 	// 	//   }
// 	return "port4 return"
// }

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
		// var port4 Port4
		// d := protocolParserPort4(b)
		// json.Unmarshal([]byte(d), &port4)

		switch measurement {
		case "WeatherStation":
			// TODO: Decode Weather Station Port4
			// var weatherStation WeatherStation
			// weatherStation.InternalTemperature =
			// weatherStation.InternalHumidity =
			// weatherStation.RainLevel =
			// weatherStation.AvgWindSpeed =
			// weatherStation.GustWindSpeed =
			// weatherStation.WindDirection =
			// weatherStation.Temperature =
			// weatherStation.Humidity =
			// weatherStation.Luminosity =
			// weatherStation.Uv =
			// weatherStation.SolaRadiation =
		default:
		}
	}
	return sb.String()
}

func parseLns(measurement string, deviceId string, direction string, etc string, message string) string {
	var sb strings.Builder
	var lns Lns
	var lnsImt LnsImt
	var lnsChirpStackV4 LnsChirpStackV4
	// var influx Influx
	// var lnsAtc LnsAtc

	if message == "" {
		// return message, errors.New("empty message to parse")
		// return influx
		return "No message to parse"
	}

	switch etc {
	case "imt":
		json.Unmarshal([]byte(message), &lnsImt)

		lns.Measurement = measurement
		lns.DeviceId = lnsImt.DevEUI
		lns.RxInfoMac_0 = lnsImt.RxInfo[0].Mac
		lns.RxInfoTime_0 = lnsImt.RxInfo[0].Time.Unix() * 1000 * 1000 * 1000
		lns.RxInfoRssi_0 = lnsImt.RxInfo[0].Rssi
		lns.RxInfoSnr_0 = lnsImt.RxInfo[0].LoRaSNR
		lns.RxInfoLat_0 = lnsImt.RxInfo[0].Latitude
		lns.RxInfoLon_0 = lnsImt.RxInfo[0].Longitude
		lns.RxInfoAlt_0 = lnsImt.RxInfo[0].Altitude
		lns.TxInfoFrequency = lnsImt.TxInfo.Frequency / 1000000
		lns.TxInfoModulation = lnsImt.TxInfo.DataRate.Modulation
		lns.TxInfoBandWidth = lnsImt.TxInfo.DataRate.Bandwidth
		lns.TxInfoSpreadFactor = lnsImt.TxInfo.DataRate.SpreadFactor
		lns.TxInfoCodeRate = lnsImt.TxInfo.CodeRate
		lns.FCnt = lnsImt.FCnt
		lns.FPort = lnsImt.FPort
		lns.FType = "uplink"
		lns.Data = lnsImt.Data

	case "chirpstackv4":
		json.Unmarshal([]byte(message), &lnsChirpStackV4)

		lns.Measurement = measurement
		lns.DeviceId = lnsChirpStackV4.DeviceInfo.DevEui
		lns.RxInfoMac_0 = lnsChirpStackV4.RxInfo[0].GatewayId
		lns.RxInfoTime_0 = lnsChirpStackV4.RxInfo[0].NsTime.UnixNano()
		lns.RxInfoRssi_0 = lnsChirpStackV4.RxInfo[0].Rssi
		lns.RxInfoSnr_0 = lnsChirpStackV4.RxInfo[0].Snr
		lns.RxInfoLat_0 = lnsChirpStackV4.RxInfo[0].Location.Latitude
		lns.RxInfoLon_0 = lnsChirpStackV4.RxInfo[0].Location.Longitude
		lns.RxInfoAlt_0 = lnsChirpStackV4.RxInfo[0].Location.Altitude
		lns.TxInfoFrequency = lnsChirpStackV4.TxInfo.Frequency / 1000000
		lns.TxInfoModulation = "LORA"
		lns.TxInfoBandWidth = lnsChirpStackV4.TxInfo.Modulation.Lora.Bandwidth
		lns.TxInfoSpreadFactor = lnsChirpStackV4.TxInfo.Modulation.Lora.SpreadingFactor
		lns.TxInfoCodeRate = lnsChirpStackV4.TxInfo.Modulation.Lora.CodeRate
		lns.FCnt = lnsChirpStackV4.FCnt
		lns.FPort = lnsChirpStackV4.FPort
		lns.FType = "uplink"
		lns.Data = lnsChirpStackV4.Data

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
		sb.WriteString(lns.Measurement)

		// Tags
		sb.WriteString(`,deviceType=LNS`)
		sb.WriteString(`,deviceId=`)
		sb.WriteString(deviceId)
		sb.WriteString(`,direction=`)
		sb.WriteString(direction)
		sb.WriteString(`,origin=`)
		sb.WriteString(etc)

		sb.WriteString(`,type=`)
		sb.WriteString(lns.FType)
		sb.WriteString(`,rxMac_0=`)
		sb.WriteString(lns.RxInfoMac_0)
		sb.WriteString(`,txModulation=`)
		sb.WriteString(lns.TxInfoModulation)
		sb.WriteString(`,txCodeRate=`)
		sb.WriteString(lns.TxInfoCodeRate)

		// Fields
		sb.WriteString(` `)
		sb.WriteString(`txFrequency=`)
		sb.WriteString(strconv.FormatFloat(lns.TxInfoFrequency, 'f', -1, 64))
		sb.WriteString(`,txBandWidth=`)
		sb.WriteString(strconv.FormatUint(uint64(lns.TxInfoBandWidth), 10))
		sb.WriteString(`,txSpreadFactor=`)
		sb.WriteString(strconv.FormatUint(uint64(lns.TxInfoSpreadFactor), 10))
		sb.WriteString(`,rxRssi_0=`)
		sb.WriteString(strconv.FormatInt(int64(lns.RxInfoRssi_0), 10))
		sb.WriteString(`,rxSnr_0=`)
		sb.WriteString(strconv.FormatFloat(lns.RxInfoSnr_0, 'f', -1, 64))
		sb.WriteString(`,rxLat_0=`)
		sb.WriteString(strconv.FormatFloat(lns.RxInfoLat_0, 'f', -1, 64))
		sb.WriteString(`,rxLon_0=`)
		sb.WriteString(strconv.FormatFloat(lns.RxInfoLon_0, 'f', -1, 64))
		sb.WriteString(`,rxAlt_0=`)
		sb.WriteString(strconv.FormatUint(uint64(lns.RxInfoAlt_0), 10))
		sb.WriteString(`,fPort=`)
		sb.WriteString(strconv.FormatUint(uint64(lns.FPort), 10))
		sb.WriteString(`,fCnt=`)
		sb.WriteString(strconv.FormatUint(uint64(lns.FCnt), 10))
		sb.WriteString(`,data="`)
		sb.WriteString(lns.Data)
		sb.WriteString(`"`)

		sb.WriteString(parseLnsMeasurement(lns.Measurement, lns.Data, lns.FPort))

		// Timestamp_ms
		sb.WriteString(` `)
		sb.WriteString(strconv.FormatInt(int64(lns.RxInfoTime_0), 10))
	}
	fmt.Printf("\n\nChirpstack %s\n\n", sb.String())
	return sb.String()
}

// func parseEvse(deviceId string, direction string, etc string, message string) string {
// 	var s string

// 	return s
// }

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
	sbMqttSubTopic.WriteString("OpenDataTelemetry/")
	sbMqttSubTopic.WriteString(ORGANIZATION)
	sbMqttSubTopic.WriteString("/")
	sbMqttSubTopic.WriteString(DEVICE_TYPE)
	sbMqttSubTopic.WriteString("/+/+/+/+")

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
	// Delivery report handler for produced messages
	go func() {
		for e := range kafkaProdClient.Events() {
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
		// 1. Input
		incoming := <-c

		// 2. Process
		// 2.1. Process Topic
		s := strings.Split(incoming[0], "/")
		measurement := s[3]
		deviceId := s[4]
		direction := s[5]
		etc := s[6]

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
				// TODO: parseLns return a influx struct

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

		// kafkaProdClient.Produce(&kafka.Message{
		// 	TopicPartition: kafka.TopicPartition{Topic: &kafkaProdTopic, Partition: kafka.PartitionAny},
		// 	Value:          []byte(kafkaMessage),
		// }, nil)

		kafkaProdClient.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaProdTopic, Partition: kafka.PartitionAny},
			Value:          []byte(kafkaMessage),
			// Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		}, nil)

		kafkaProdClient.Flush(15 * 1000)
	}
}
