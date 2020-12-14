module github.com/open-telemetry/opentelemetry-collector-contrib/exporter/stackdriverexporter

go 1.14

require (
	contrib.go.opencensus.io/exporter/stackdriver v0.13.2
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace v0.2.2-0.20200728233621-2752da7eaab7
	github.com/golang/protobuf v1.4.2
	github.com/stretchr/testify v1.6.1
	go.opencensus.io v0.22.4
	go.opentelemetry.io/collector v0.7.1-0.20200807172154-11198b124dae
	go.opentelemetry.io/otel v0.15.0 // indirect
	go.opentelemetry.io/otel/sdk v0.15.0
	go.uber.org/zap v1.15.0
	google.golang.org/api v0.29.0
	google.golang.org/genproto v0.0.0-20200715011427-11fb19a81f2c
	google.golang.org/grpc v1.31.0
	google.golang.org/grpc/examples v0.0.0-20200728194956-1c32b02682df // indirect
)
