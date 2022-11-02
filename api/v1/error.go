package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

type ErrorOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrorOffsetOutOfRange) GRPCStatus() *status.Status {
	st := status.New(404, fmt.Sprintf("offset out of range: %d", e.Offset))
	msg := fmt.Sprintf("The requrested offset is outside the log's range: %d", e.Offset)
	d := &errdetails.LocalizedMessage{Locale: "en-us", Message: msg}
	if std, err := st.WithDetails(d); err != nil {
		return st
	} else {
		return std
	}
}

func (e ErrorOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
