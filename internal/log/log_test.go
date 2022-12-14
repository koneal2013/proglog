package log_test

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	log_v1 "github.com/koneal2013/proglog/api/v1"
	logpkg "github.com/koneal2013/proglog/internal/log"
)

const (
	lenWidth = 8
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *logpkg.Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp(os.TempDir(), "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := logpkg.Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := logpkg.NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testTruncate(t *testing.T, log *logpkg.Log) {
	append := &log_v1.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}
	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)
}

func testReader(t *testing.T, log *logpkg.Log) {
	append := &log_v1.Record{Value: []byte("hello world")}
	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)

	read := &log_v1.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)

}

func testInitExisting(t *testing.T, log *logpkg.Log) {
	append := &log_v1.Record{Value: []byte("hello world")}

	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	n, err := logpkg.NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	off, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

}

func testOutOfRangeErr(t *testing.T, log *logpkg.Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	apiErr := err.(log_v1.ErrorOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset)

}

func testAppendRead(t *testing.T, log *logpkg.Log) {
	append := &log_v1.Record{Value: []byte("hello world")}
	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
}
