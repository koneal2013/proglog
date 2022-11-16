package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/koneal2013/proglog/internal/auth"
	"github.com/koneal2013/proglog/internal/discovery"
	"github.com/koneal2013/proglog/internal/log"
	"github.com/koneal2013/proglog/internal/observability"
	"github.com/koneal2013/proglog/internal/server"
)

type Config struct {
	ServerTLSConfig       *tls.Config
	PeerTLSConfig         *tls.Config
	DataDir               string
	BindAddr              string
	RPCPort               int
	NodeName              string
	StartJoinAddrs        []string
	ACLModelFile          string
	ACLPolicyFile         string
	OTPLCollectorURL      string
	OTPLCollectorInsecure bool
	IsDevelopment         bool
	Bootstrap             bool
}
type Agent struct {
	Config

	mux           cmux.CMux
	log           *log.DistributedLog
	server        *grpc.Server
	membership    *discovery.Membership
	traceProvider *trace.TracerProvider

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

func (a *Agent) setupLogger() error {
	if logger, err := observability.NewLogger(a.Config.IsDevelopment, "proglog"); err != nil {
		return err
	} else {
		zap.ReplaceGlobals(logger.Named(a.Config.NodeName))
	}
	return nil
}

func (a *Agent) setupLog() error {
	raftLn := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Compare(b, []byte{log.RaftRPC}) == 0
	})
	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(raftLn, a.Config.ServerTLSConfig, a.Config.PeerTLSConfig)
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap
	var err error
	a.log, err = log.NewDistributedLog(a.Config.DataDir, logConfig)
	if err != nil {
		return err
	}
	if a.Config.Bootstrap {
		err = a.log.WaitForLeader(3 * time.Second)
	}
	return err
}

func (a *Agent) setupServer() error {
	if authorizer, err := auth.New(a.Config.ACLModelFile, a.Config.ACLPolicyFile); err != nil {
		return err
	} else if tp, err := observability.NewTrace(fmt.Sprintf("proglog.%s", a.Config.NodeName), a.Config.OTPLCollectorURL, a.Config.OTPLCollectorInsecure); err != nil {
		return err
	} else {
		a.traceProvider = tp
		serverConfig := &server.Config{CommitLog: a.log, Authorizer: authorizer}
		var opts []grpc.ServerOption
		if a.Config.ServerTLSConfig != nil {
			creds := credentials.NewTLS(a.Config.ServerTLSConfig)
			opts = append(opts, grpc.Creds(creds))
		}
		var err error
		a.server, err = server.NewGRPCServer(serverConfig, opts...)
		if err != nil {
			return err
		} else {
			grpcLn := a.mux.Match(cmux.Any())
			go func() {
				if err := a.server.Serve(grpcLn); err != nil {
					_ = a.Shutdown()
				}
			}()
			return err
		}
	}
	return nil
}

func (a *Agent) setupMembership() error {
	if rpcAddr, err := a.Config.RPCAddr(); err != nil {
		return err
	} else {
		a.membership, err = discovery.New(a.log, discovery.Config{
			NodeName: a.Config.NodeName,
			BindAddr: a.Config.BindAddr,
			Tags: map[string]string{
				"rpc_addr": rpcAddr,
			},
			StartJoinAddrs: a.Config.StartJoinAddrs,
		})
		return err
	}
	return nil
}

func (a *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(":%d", a.Config.RPCPort)
	if ln, err := net.Listen("tcp", rpcAddr); err != nil {
		return err
	} else {
		a.mux = cmux.New(ln)
	}
	return nil
}

func (c *Config) RPCAddr() (string, error) {
	if host, _, err := net.SplitHostPort(c.BindAddr); err != nil {
		return "", err
	} else {
		return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
	}
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)
	a.traceProvider.Shutdown(context.Background())

	shutdown := []func() error{
		a.membership.Leave,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	go a.serve()
	return a, nil
}
