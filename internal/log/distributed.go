package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	api "github.com/koneal2013/proglog/api/v1"
)

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {

}

var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		if _, err := io.ReadFull(r, b); err == io.EOF {
			break
		} else if err != nil {
			return err
		} else {
			size := int64(enc.Uint64(b))
			if _, err := io.CopyN(&buf, r, size); err != nil {
				return err
			} else {
				record := &api.Record{}
				if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
					return err
				} else if i == 0 {
					f.log.Config.Segment.InitialOffSet = record.Offset
					if err := f.log.Reset(); err != nil {
						return err
					}
				} else if _, err := f.log.Append(record); err != nil {
					return err
				}
				buf.Reset()
			}
		}
	}
	return nil
}

func (f *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return f.applyAppend(buf[1:])
	}
	return nil
}

func (f *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	if err := proto.Unmarshal(b, &req); err != nil {
		return err
	} else if offset, err := f.log.Append(req.Record); err != nil {
		return err
	} else {
		return &api.ProduceResponse{Offset: offset}
	}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	if log, err := NewLog(dir, c); err != nil {
		return nil, err
	} else {
		return &logStore{log}, nil
	}
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	return l.HighestOffset()
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	if in, err := l.Read(index); err != nil {
		return err
	} else {
		out.Data = in.Value
		out.Index = in.Offset
		out.Type = raft.LogType(in.Type)
		out.Term = in.Term
	}
	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	l.log, err = NewLog(logDir, l.config)
	return err
}

func (l *DistributedLog) setupRaft(dataDir string) error {
	fsm := &fsm{log: l.log}

	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	logConfig := l.config
	logConfig.Segment.InitialOffSet = 1
	logWriter := zap.NewStdLog(zap.L()).Writer()
	if logStore, err := newLogStore(logDir, logConfig); err != nil {
		return err
	} else {
		if stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable")); err != nil {
			return err
		} else {
			retain := 1
			if snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "raft"),
				retain,
				logWriter,
			); err != nil {
				return err
			} else {
				maxPool := 5
				timeout := 10 * time.Second
				transport := raft.NewNetworkTransport(
					l.config.Raft.StreamLayer,
					maxPool,
					timeout,
					logWriter,
				)
				config := raft.DefaultConfig()
				config.LocalID = l.config.Raft.LocalID
				if l.config.Raft.HeartbeatTimeout != 0 {
					config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
				}
				if l.config.Raft.ElectionTimeout != 0 {
					config.ElectionTimeout = l.config.Raft.ElectionTimeout
				}
				if l.config.Raft.LeaderLeaseTimeout != 0 {
					config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
				}
				if l.config.Raft.CommitTimeout != 0 {
					config.CommitTimeout = l.config.Raft.CommitTimeout
				}
				l.raft, err = raft.NewRaft(
					config,
					fsm,
					logStore,
					stableStore,
					snapshotStore,
					transport,
				)
				if err != nil {
					return err
				}
				if hasState, err := raft.HasExistingState(
					logStore,
					stableStore,
					snapshotStore,
				); err != nil {
					return err
				} else if l.config.Raft.Bootstrap && !hasState {
					config := raft.Configuration{
						Servers: []raft.Server{{
							ID:      config.LocalID,
							Address: transport.LocalAddr(),
						}},
					}
					err = l.raft.BootstrapCluster(config).Error()
				}
				return err
			}
		}
	}

}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	l := &DistributedLog{config: config}
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (interface{}, error) {
	var buf bytes.Buffer
	if _, err := buf.Write([]byte{byte(reqType)}); err != nil {
		return nil, err
	} else if b, err := proto.Marshal(req); err != nil {
		return nil, err
	} else if _, err = buf.Write(b); err != nil {
		return nil, err
	} else {
		timeout := 10 * time.Second
		if future := l.raft.Apply(buf.Bytes(), timeout); future.Error() != nil {
			return nil, future.Error()
		} else {
			res := future.Response()
			if err, ok := res.(error); ok {
				return nil, err
			}
			return res, nil
		}
	}
}

func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	if res, err := l.apply(AppendRequestType, &api.ProduceRequest{Record: record}); err != nil {
		return 0, err
	} else {
		return res.(*api.ProduceResponse).Offset, nil
	}
}

func (l *DistributedLog) Join(id, addr string) error {
	configFuture := l.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				// server has already joined
				return nil
			}
			// remove the existing server id / address association
			removeFuture := l.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}
	addFuture := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}
	return nil
}

func (l *DistributedLog) Leave(id string) error {
	removeFuture := l.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if laddr, _ := l.raft.LeaderWithID(); laddr != "" {
				return nil
			}
		}
	}
}

func (l *DistributedLog) Close() error {
	f := l.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return l.log.Close()
}

func (l *DistributedLog) GetServers() ([]*api.Server, error) {
	future := l.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	var servers []*api.Server
	for _, server := range future.Configuration().Servers {
		leaderAddr, _ := l.raft.LeaderWithID()
		servers = append(servers, &api.Server{
			Id:       string(server.ID),
			RpcAddr:  string(server.Address),
			IsLeader: leaderAddr == server.Address,
		})
	}
	return servers, nil
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(ln net.Listener, serverTLSConfig, peerTLSConfig *tls.Config) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

const RaftRPC = 1

func (s *StreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (conn net.Conn, err error) {
	dialer := &net.Dialer{Timeout: timeout}
	if conn, err = dialer.Dial("tcp", string(addr)); err != nil {
		return nil, err
	} else if _, err = conn.Write([]byte{byte(RaftRPC)}); err != nil {
		return nil, err
	} else if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}
	return conn, nil
}

func (s *StreamLayer) Accept() (conn net.Conn, err error) {
	if conn, err = s.ln.Accept(); err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	if _, err = conn.Read(b); err != nil {
		return nil, err
	} else if bytes.Compare([]byte{byte(RaftRPC)}, b) != 0 {
		return nil, fmt.Errorf("not a raft rpc")
	} else if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}
	return conn, nil
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
