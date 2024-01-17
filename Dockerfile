# Build the application from source
# FROM golang:1.21.6-alpine3.19 AS build-stage
FROM golang:1.21 AS build-stage

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /gateway-mqtt

# Run the tests in the container
# FROM build-stage AS run-test-stage
# RUN go test -v ./...

# Deploy the application binary into a lean image
FROM gcr.io/distroless/base-debian12 AS build-release-stage

WORKDIR /

COPY --from=build-stage /gateway-mqtt /gateway-mqtt

USER nonroot:nonroot

ENTRYPOINT ["/gateway-mqtt"]
