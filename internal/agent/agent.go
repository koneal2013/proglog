package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	api "github.com/koneal2013/proglog/api/v1"
	"github.com/koneal2013/proglog/internal/auth"
	"github.com/koneal2013/proglog/internal/discovery"
	"github.com/koneal2013/proglog/internal/log"
	"github.com/koneal2013/proglog/internal/server"
)

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	PRCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
}
type Agent struct {
	Config

	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

func (a *Agent) setupLogger() error {
	if logger, err := zap.NewDevelopment(); err != nil {
		return err
	} else {
		zap.ReplaceGlobals(logger)
	}
	return nil
}

func (a *Agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(a.Config.DataDir, log.Config{})
	return err
}

func (a *Agent) setupServer() error {
	if authorizer, err := auth.New(a.Config.ACLModelFile, a.Config.ACLPolicyFile); err != nil {
		return err
	} else {
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
		}
		if rpcAddr, err := a.RPCAddr(); err != nil {
			return err
		} else if ln, err := net.Listen("tcp", rpcAddr); err != nil {
			return err
		} else {
			go func() {
				if err := a.server.Serve(ln); err != nil {
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
		var opts []grpc.DialOption
		if a.Config.PeerTLSConfig != nil {
			opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(a.Config.PeerTLSConfig)))
		}
		if conn, err := grpc.Dial(rpcAddr, opts...); err != nil {
			return err
		} else {
			client := api.NewLogClient(conn)
			a.replicator = &log.Replicator{
				DialOptions: opts,
				LocalServer: client,
			}
			a.membership, err = discovery.New(a.replicator, discovery.Config{
				NodeName: a.Config.NodeName,
				BindAddr: a.Config.BindAddr,
				Tags: map[string]string{
					"rpc_addr": rpcAddr,
				},
				StartJoinAddrs: a.Config.StartJoinAddrs,
			})
			return err
		}
	}
	return nil
}

func (c *Config) RPCAddr() (string, error) {
	if host, _, err := net.SplitHostPort(c.BindAddr); err != nil {
		return "", err
	} else {
		return fmt.Sprintf("%s:%d", host, c.PRCPort), nil
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

	shutdown := []func() error{
		a.membership.Leave,
		a.replicator.Close,
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

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		a.setupLogger,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	return a, nil
}
