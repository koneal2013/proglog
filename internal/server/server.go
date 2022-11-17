package server

import (
	"context"
	"time"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	otel_codes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	peer2 "google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"

	api "github.com/koneal2013/proglog/api/v1"
)

const (
	objectWildCard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type Config struct {
	CommitLog   CommitLog
	Authorizer  Authorizer
	GetServerer GetServerer
}

var _ api.LogServer = (*grpcServer)(nil)

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	logger := zap.L().Named("server")
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64("grpc.time_ns", duration.Nanoseconds())
			}),
	}
	opts = append(opts, grpc.StreamInterceptor(
		grpcmiddleware.ChainStreamServer(
			grpc_ctxtags.StreamServerInterceptor(),
			grpc_zap.StreamServerInterceptor(logger, zapOpts...),
			grpcauth.StreamServerInterceptor(authenticate),
			otelgrpc.StreamServerInterceptor(),
		)), grpc.UnaryInterceptor(grpcmiddleware.ChainUnaryServer(
		grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
		grpcauth.UnaryServerInterceptor(authenticate),
		otelgrpc.UnaryServerInterceptor(),
	)))
	gsrv := grpc.NewServer(opts...)
	if srv, err := newGrpcServer(config); err != nil {
		return nil, err
	} else {
		api.RegisterLogServer(gsrv, srv)
		return gsrv, nil
	}
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newGrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{Config: config}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	span := trace.SpanFromContext(ctx)
	if err := s.Authorizer.Authorize(subject(ctx), objectWildCard, produceAction); err != nil {
		span.RecordError(err)
		span.SetStatus(otel_codes.Error, err.Error())
		return nil, err
	}
	if offset, err := s.CommitLog.Append(req.Record); err != nil {
		span.RecordError(err)
		span.SetStatus(otel_codes.Error, err.Error())
		return nil, err
	} else {
		return &api.ProduceResponse{Offset: offset}, nil
	}
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	span := trace.SpanFromContext(ctx)
	if err := s.Authorizer.Authorize(subject(ctx), objectWildCard, consumeAction); err != nil {
		span.RecordError(err)
		span.SetStatus(otel_codes.Error, err.Error())
		return nil, err
	}
	if record, err := s.CommitLog.Read(req.Offset); err != nil {
		span.RecordError(err)
		span.SetStatus(otel_codes.Error, err.Error())
		return nil, err
	} else {
		return &api.ConsumeResponse{Record: record}, nil
	}
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	span := trace.SpanFromContext(stream.Context())
	for {
		if req, err := stream.Recv(); err != nil {
			span.RecordError(err)
			span.SetStatus(otel_codes.Error, err.Error())
			return err
		} else if res, err := s.Produce(stream.Context(), req); err != nil {
			return err
		} else if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	span := trace.SpanFromContext(stream.Context())
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrorOffsetOutOfRange:
				continue
			default:
				span.RecordError(err)
				span.SetStatus(otel_codes.Error, err.Error())
				return err
			}
			if err = stream.Send(res); err != nil {
				span.RecordError(err)
				span.SetStatus(otel_codes.Error, err.Error())
				return err
			}
			req.Offset++
		}
	}
}

func (s *grpcServer) GetServers(ctx context.Context, req *api.GetServersRequest) (*api.GetServersResponse, error) {
	if servers, err := s.GetServerer.GetServers(); err != nil {
		return nil, err
	} else {
		return &api.GetServersResponse{Servers: servers}, nil
	}
}

func authenticate(ctx context.Context) (context.Context, error) {
	if peer, ok := peer2.FromContext(ctx); !ok {
		return ctx, status.New(codes.Unknown, "couldn't find peer info").Err()
	} else if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	} else {
		tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
		subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
		ctx = context.WithValue(ctx, subjectContextKey{}, subject)
		return ctx, nil
	}
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)

}

type subjectContextKey struct{}

type CommitLog interface {
	Append(record *api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type GetServerer interface {
	GetServers() ([]*api.Server, error)
}
