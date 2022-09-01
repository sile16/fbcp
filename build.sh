#!/bin/bash
# change default to linux.
env GOOS=linux GOARCH=amd64 go build -o fbcp
# keep this binary incase someone has a link to here.
env GOOS=linux GOARCH=amd64 go build -o fbcp_linux
env GOOS=darwin GOARCH=arm64 go build -o fbcp_mac 