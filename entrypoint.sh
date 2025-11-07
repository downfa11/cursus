#!/bin/bash
set -e

# start broker server in background
./broker -config ./config.yaml &
sleep 1

# start CLI with config
exec ./cli -config ./config.yaml
