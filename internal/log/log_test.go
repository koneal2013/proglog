package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
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

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testTruncate(t *testing.T, log *Log) {

}

func testReader(t *testing.T, log *Log) {

}

func testInitExisting(t *testing.T, log *Log) {

}

func testOutOfRangeErr(t *testing.T, log *Log) {

}

func testAppendRead(t *testing.T, log *Log) {

}
