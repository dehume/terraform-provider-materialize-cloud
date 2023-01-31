NAME=materialize
BINARY=terraform-provider-${NAME}
PLUGIN_PATH=~/.terraform.d/plugins/materialize.com/devex/materialize/0.1/darwin_arm64

default: testacc

.PHONY: fmt
fmt:
	gofmt -l -s -w .
	terraform fmt -recursive

.PHONY: test
test:
	TF_ACC=1 go test ./... -v $(TESTARGS) -timeout 120m

.PHONY: build
build:
	go build -o ${BINARY}
	mkdir -p ${PLUGIN_PATH}
	mv ${BINARY} ${PLUGIN_PATH}
	rm examples/.terraform.lock.hcl
