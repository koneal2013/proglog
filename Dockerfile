FROM golang:1.19-alpine as build
COPY . /usr/src/server/
WORKDIR /usr/src/server/
RUN go env -w GOPROXY=direct
RUN go install ./main.go
FROM alpine:3.15
COPY --from=build /go/bin/main  /go/bin/main
CMD ["/go/bin/main"]
