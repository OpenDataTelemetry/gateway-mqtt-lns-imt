module github.com/OpenDataTelemetry/device-gateway-mqtt

go 1.19

require (
	github.com/confluentinc/confluent-kafka-go v1.9.2
	github.com/eclipse/paho.mqtt.golang v1.4.2
	github.com/google/uuid v1.3.1
	github.com/pinpt/go-common v9.1.81+incompatible
)

require (
	github.com/OpenDataTelemetry/decode v0.0.0-20240116143143-7c5d6dea47d5
	github.com/gorilla/websocket v1.4.2 // indirect
	golang.org/x/net v0.17.0 // indirect
	golang.org/x/sync v0.3.0 // indirect
)

replace github.com/OpenDataTelemetry/decode => ../decode
