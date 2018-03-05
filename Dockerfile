FROM golang

WORKDIR /go/src/github.com/BaritoLog/barito-flow
COPY . .
RUN go get
RUN go build
RUN go install
COPY entrypoint.sh /go
EXPOSE 8080

CMD ["/go/entrypoint.sh"]
