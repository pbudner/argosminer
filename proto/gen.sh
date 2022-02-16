#!/bin/bash
set -e

# Makesure protoc is added to PATH
PROTOC=protoc # provide path to protoc binary
PROTO=argosminer.proto
GOBIN=$(go env GOPATH)/bin
GOSRC=$(go env GOPATH)/src
ARGOSMINER_ROOT=$GOSRC/github.com/pbudner/argosminer
PROTO_PATH=$ARGOSMINER_ROOT/proto

$PROTOC -I $PROTO_PATH --go_out=$GOSRC $PROTO