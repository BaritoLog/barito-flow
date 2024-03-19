FROM --platform=arm64 golang:1.20-alpine as builder

WORKDIR /go/src/github.com/BaritoLog/barito-flow
COPY . .

RUN go env -w GOARCH=amd64
RUN go build -o barito-flow

FROM scratch

WORKDIR /app
COPY --from=builder /go/src/github.com/BaritoLog/barito-flow /app/

ENTRYPOINT ["/app/barito-flow"]

EXPOSE 8080
