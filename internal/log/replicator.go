package log

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	api "github.com/koneal2013/proglog/api/v1"
	"github.com/koneal2013/proglog/internal/observability"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient

	logger observability.LoggingSystem

	mu      sync.Mutex
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}
	if _, ok := r.servers[name]; ok {
		// already replicating so skip
		return nil
	}
	r.servers[name] = make(chan struct{})

	go r.replicate(addr, r.servers[name])
	return nil
}

func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	if cc, err := grpc.Dial(addr, r.DialOptions...); err != nil {
		r.logError(err, "failed to dial", addr)
		return
	} else {
		defer cc.Close()

		client := api.NewLogClient(cc)

		ctx := context.Background()
		if stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0}); err != nil {
			r.logError(err, "failed to consume", addr)
			return
		} else {
			records := make(chan *api.Record)
			go func() {
				for {
					if recv, err := stream.Recv(); err != nil {
						r.logError(err, "failed to receive", addr)
					} else {
						records <- recv.Record
					}
				}
			}()
			for {
				select {
				case <-r.close:
					return
				case <-leave:
					return
				case record := <-records:
					if _, err = r.LocalServer.Produce(ctx, &api.ProduceRequest{Record: record}); err != nil {
						r.logError(err, "failed to produce", addr)
						return
					}
				}
			}
		}
	}
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

func (r *Replicator) logError(err error, msg string, addr string) {
	r.logger.Error(msg, zap.String("addr", addr), zap.Error(err))

}
