FROM ykode/golang-builder as builder

WORKDIR /go/src/github.com/BaritoLog/barito-flow
COPY . . 

ARG COMMIT

RUN dep ensure && \
    CC=clang CXX=clang++ CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
    go build \
    -ldflags "-X main.Commit=${COMMIT} -X main.Build=$(date +%Y%m%d) -s -w -linkmode external -extldflags \"-static\"" \
    -o barito-flow

FROM  scratch

ADD https://curl.haxx.se/ca/cacert.pem /etc/ssl/certs/ca-certificate.crt

WORKDIR /app
COPY --from=builder /go/src/github.com/BaritoLog/barito-flow /app/

ENTRYPOINT ["/app/barito-flow"]

CMD ["producer"]

EXPOSE 8080
