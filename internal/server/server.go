package server

import (
	"context"

	api "github.com/koneal2013/proglog/api/v1"
	"github.com/koneal2013/proglog/internal/log"
)

type Config struct {
	CommitLog log.Log
}

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newGrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{Config: config}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	if offset, err := s.CommitLog.Append(req.Record); err != nil {
		return nil, err
	} else {
		return &api.ProduceResponse{Offset: offset}, nil
	}
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if record, err := s.CommitLog.Read(req.Offset); err != nil {
		return nil, err
	} else {
		return &api.ConsumeResponse{Record: record}, nil
	}
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		if req, err := stream.Recv(); err != nil {
			return err
		} else if res, err := s.Produce(stream.Context(), req); err != nil {
			return err
		} else if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
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
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}
