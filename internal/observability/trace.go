package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/koneal2013/proglog/internal/config"
)

// NewTrace configures an OpenTelemetry exporter and trace provider
func NewTrace(serviceName, collectorURL string, log LoggingSystem, insecure bool) (traceProvider *sdktrace.TracerProvider, err error) {
	tlsConfig, _ := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ServerCertFile,
		KeyFile:  config.ServerKeyFile,
		CAFile:   config.CAFile,
		Server:   true,
	})

	secureOption := otlptracegrpc.WithTLSCredentials(credentials.NewTLS(tlsConfig))
	if insecure {
		secureOption = otlptracegrpc.WithInsecure()
	}
	if exporter, err := otlptrace.New(
		context.Background(),
		otlptracegrpc.NewClient(
			secureOption,
			otlptracegrpc.WithEndpoint(collectorURL),
			otlptracegrpc.WithDialOption(grpc.WithBlock()),
		),
	); err != nil {
		log.Sugar().Fatal(err)
	} else if resources, err := resource.New(
		context.Background(),
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(
			attribute.String("service.name", serviceName),
		),
	); err != nil {
		log.Sugar().Errorf("Could not set resources: %v", err)
		return nil, err
	} else {

		bsp := sdktrace.NewBatchSpanProcessor(exporter)

		traceProvider = sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithSpanProcessor(bsp),
			sdktrace.WithResource(resources),
		)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
		otel.SetTracerProvider(traceProvider)
	}
	return traceProvider, nil
}
