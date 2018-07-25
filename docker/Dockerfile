FROM golang:alpine as builder

WORKDIR /go/src/github.com/BaritoLog/barito-flow
COPY . .

ARG COMMIT

RUN export BUILD_PACKAGES="upx file clang build-base curl git bzr mercurial" && export OUTPUT="barito-flow" \
      && apk --update --no-cache add ${BUILD_PACKAGES} \
      && go get \
      && CC=clang CXX=clang++ CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build \
         -ldflags "-X main.Commit=${COMMIT} -X main.Build=$(date +%Y%m%d) -s -w -linkmode external -extldflags \"-static\"" -o ${OUTPUT} \
      && apk del ${BUILD_PACKAGES} \
      && unset BUILD_PACKAGES && unset OUTPUT


FROM  scratch

ADD https://curl.haxx.se/ca/cacert.pem /etc/ssl/certs/ca-certificate.crt

WORKDIR /app
COPY --from=builder /go/src/github.com/BaritoLog/barito-flow /app/

ENTRYPOINT ["/app/barito-flow"]

CMD ["/app/barito-flow"]

EXPOSE 8080
