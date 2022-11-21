FROM golang:1.19-alpine as build
# Set the working directory
WORKDIR /go/src/proglog
# Copy and download dependencies using go mod
COPY go.mod .
COPY go.sum .
RUN go mod download
# Copy the source files from the host
COPY . /go/src/proglog

RUN CGO_ENABLED=0 GOOS=linux go build -o /go/bin/proglog ./cmd/proglog
RUN GRPC_HEALTH_PROBE_VERSION=v0.3.2 && \
    wget -q0/go/bin/grpc_health_probe \
    https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/ \
    ${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /go/bin/grpc_health_probe \

FROM scratch
COPY --from=build /go/bin/proglog  /bin/proglog
COPY --from=build /go/bin/grpc_health_probe /bin/grpc_health_probe
CMD ["/bin/proglog"]
