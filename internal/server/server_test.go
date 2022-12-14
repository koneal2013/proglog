package server

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	api "github.com/koneal2013/proglog/api/v1"
	"github.com/koneal2013/proglog/internal/auth"
	"github.com/koneal2013/proglog/internal/config"
	"github.com/koneal2013/proglog/internal/log"
	"github.com/koneal2013/proglog/internal/observability"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient, nobodyClient api.LogClient,
		config *Config,
		tp trace.TracerProvider,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"unauthorized fails":                                 testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			tpServer, err := observability.NewTrace("test.proglog", "localhost:4317", true)
			require.NoError(t, err)
			defer func(t *testing.T, tp *sdktrace.TracerProvider, ctx context.Context) {
				err := tp.Shutdown(ctx)
				require.NoError(t, err)
			}(t, tpServer, context.Background())

			tpClient, err := observability.NewTrace("test.proglog", "localhost:4317", true)
			require.NoError(t, err)
			defer func(t *testing.T, tp *sdktrace.TracerProvider, ctx context.Context) {
				err := tp.Shutdown(ctx)
				require.NoError(t, err)
			}(t, tpClient, context.Background())
			rootClient, nobodyClient, config, teardown := setupTest(t, tpClient, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config, tpClient)
		})
	}
}

func setupTest(t *testing.T, tpClient trace.TracerProvider, fn func(*Config)) (rootClient, nobodyClient api.LogClient, cfg *Config, teardown func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds), grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor(otelgrpc.WithTracerProvider(tpClient))),
			grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor(otelgrpc.WithTracerProvider(tpClient)))}
		conn, err := grpc.Dial(l.Addr().String(), opts...)
		require.NoError(t, err)
		client := api.NewLogClient(conn)
		return conn, client, opts
	}

	rootConn, rootClient, _ := newClient(config.RootClientCertFile, config.RootClientKeyFile)
	nobodyConn, nobodyClient, _ := newClient(config.NobodyClientCertFile, config.NobodyClientKeyFile)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp(os.TempDir(), "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer, err := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	require.NoError(t, err)

	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}
	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		clog.Remove()
	}
}

func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, config *Config, tp trace.TracerProvider) {
	ctx, span := tp.Tracer("test").Start(context.Background(), "testConsumePastBoundary")
	defer span.End()
	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}})
	require.NoError(t, err)

	if consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1}); consume != nil {
		t.Fatal("consume not nil")
	} else {
		got := status.Code(err)
		want := status.Code(api.ErrorOffsetOutOfRange{}.GRPCStatus().Err())
		if got != want {
			t.Fatalf("got err: %v, want: %v", got, want)
		}
	}

}

func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config, tp trace.TracerProvider) {
	ctx, span := tp.Tracer("test").Start(context.Background(), "testProduceConsume")
	defer span.End()
	want := &api.Record{Value: []byte("hello world")}
	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config, tp trace.TracerProvider) {
	ctx, span := tp.Tracer("test").Start(context.Background(), "testProduceConsumeStream")
	defer span.End()
	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		}, {
			Value:  []byte("second message"),
			Offset: 1,
		},
	}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", res.Offset, offset)
			}
		}
	}
	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)
		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record.Value, record.Value)
			require.Equal(t, res.Record.Offset, uint64(i))
		}
	}
}

func testUnauthorized(t *testing.T, _, client api.LogClient, config *Config, tp trace.TracerProvider) {
	ctx, span := tp.Tracer("test").Start(context.Background(), "testUnauthorized")
	defer span.End()
	if produce, err := client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}}); produce != nil {
		t.Fatalf("produce response should be nil")
	} else {
		gotCode, wantCode := status.Code(err), codes.PermissionDenied
		if gotCode != wantCode {
			t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
		}
		if consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0}); consume != nil {
			t.Fatalf("consume response should be nil")
		} else {
			gotCode, wantCode = status.Code(err), codes.PermissionDenied
			if gotCode != wantCode {
				t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
			}
		}
	}
}
