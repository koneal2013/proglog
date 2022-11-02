package log

import (
	"fmt"
	"os"
	"path"

	"google.golang.org/protobuf/proto"

	log_v1 "github.com/koneal2013/proglog/api/v1"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	if storeFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644); err != nil {
		return nil, err
	} else if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	if indexFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")), os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return nil, err
	} else if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

func (s *segment) Append(record *log_v1.Record) (offset uint64, err error) {
	offset = s.nextOffset
	record.Offset = offset
	if p, err := proto.Marshal(record); err != nil {
		return 0, err
	} else if _, pos, err := s.store.Append(p); err != nil {
		return 0, err
	} else if err = s.index.Write(uint32(s.nextOffset-s.baseOffset), pos); err != nil {
		return 0, err
	}
	s.nextOffset++
	return offset, nil
}

func (s *segment) Read(off uint64) (*log_v1.Record, error) {
	if _, pos, err := s.index.Read(int64(off - s.baseOffset)); err != nil {
		return nil, err
	} else if p, err := s.store.Read(pos); err != nil {
		return nil, err
	} else {
		record := &log_v1.Record{}
		err = proto.Unmarshal(p, record)
		return record, err
	}
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
