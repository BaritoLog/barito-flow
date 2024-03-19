.PHONY: docker

GIT ?= $(shell which git)
COMMIT ?= $(shell $(GIT) rev-parse --verify HEAD | cut -c1-8)
DOCKER ?= $(shell which docker)
DOCKER_TAG ?= "baritolog/barito-flow"

BINARY ?= "barito-flow"

all: $(BINARY)

$(BINARY): main.go
	@printf "Building $(BINARY)..\n"
	go build -o $@

docker: docker/Dockerfile
	@printf "Building docker image \033[31;1m%s\033[0m...\n" $(DOCKER_TAG)
	$(DOCKER) build -f $^ -t $(DOCKER_TAG) --build-arg COMMIT=$(COMMIT) .

mockgen:
	mockgen -source=./flow/types/types.go -package=mock  -destination=./mock/flow_types.go

test:
	go test -v ./flow

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@latest .

deadcode:
	go run golang.org/x/tools/cmd/deadcode@latest .
