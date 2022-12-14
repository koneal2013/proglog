package loadbalance

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	api "github.com/koneal2013/proglog/api/v1"
)

const Name = "proglog"

type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

var _ resolver.Builder = (*Resolver)(nil)

func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(opts.DialCreds))
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(fmt.Sprintf(`{"loadbalancingConfig": [{"%s": {}}]}`, Name))
	var err error
	r.resolverConn, err = grpc.Dial(target.URL.Host, dialOpts...)
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

func (r *Resolver) Scheme() string {
	return Name
}

func init() {
	resolver.Register(&Resolver{})
}

var _ resolver.Resolver = (*Resolver)(nil)

func (r *Resolver) ResolveNow(options resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	client := api.NewLogClient(r.resolverConn)

	ctx := context.Background()
	if res, err := client.GetServers(ctx, &api.GetServersRequest{}); err != nil {
		r.logger.Error("failed to resolver server", zap.Error(err))
		return
	} else {
		var addrs []resolver.Address
		for _, server := range res.Servers {
			addrs = append(addrs, resolver.Address{
				Addr:       server.RpcAddr,
				ServerName: server.Id,
				Attributes: attributes.New("is_leader", server.IsLeader),
			})
		}
		r.clientConn.UpdateState(resolver.State{
			Addresses:     addrs,
			ServiceConfig: r.serviceConfig,
		})
	}
}

func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error("failed to close conn", zap.Error(err))
	}

}
