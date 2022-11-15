package log

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"

	api "github.com/koneal2013/proglog/api/v1"
)

var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

type snapshot struct {
	reader io.Reader
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {

}

func (l *fsm) Restore(r io.ReadCloser) error {
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
					l.log.Config.Segment.InitialOffSet = record.Offset
					if err := l.log.Reset(); err != nil {
						return err
					}
				} else if _, err := l.log.Append(record); err != nil {
					return err
				}
				buf.Reset()
			}
		}
	}
	return nil
}

func (l *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return l.applyAppend(buf[1:])
	}
	return nil
}

func (l *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	if err := proto.Unmarshal(b, &req); err != nil {
		return err
	} else if offset, err := l.log.Append(req.Record); err != nil {
		return err
	} else {
		return &api.ProduceResponse{Offset: offset}
	}
}

func (l *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := l.log.Reader()
	return &snapshot{reader: r}, nil
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
	if logStore, err := newLogStore(logDir, logConfig); err != nil {
		return err
	} else {
		if stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable")); err != nil {
			return err
		} else {
			retain := 1
			if snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "raft"),
				retain,
				os.Stderr,
			); err != nil {
				return err
			} else {
				maxPool := 5
				timeout := 10 * time.Second
				transport := raft.NewNetworkTransport(
					l.config.Raft.StreamLayer,
					maxPool,
					timeout,
					os.Stderr,
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
