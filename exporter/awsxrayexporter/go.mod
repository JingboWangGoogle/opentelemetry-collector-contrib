module github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awsxrayexporter

go 1.14

require (
	github.com/aws/aws-sdk-go v1.39.4
	github.com/open-telemetry/opentelemetry-proto v0.4.0
	github.com/stretchr/testify v1.6.1
	go.opentelemetry.io/collector v0.7.1-0.20200807172154-11198b124dae
	go.uber.org/zap v1.15.0
	golang.org/x/net v0.0.0-20210614182718-04defd469f4e
	google.golang.org/grpc/examples v0.0.0-20200728194956-1c32b02682df // indirect
)
