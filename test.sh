#!/bin/bash

set -ev

echo "mode: atomic" > coverage.txt
ls
pwd
goapp list ./... | xargs -n1 -I{} sh -c 'goapp test -covermode=atomic -coverprofile=coverage.tmp {} && tail -n +2 coverage.tmp >> coverage.txt' 
ls
rm coverage.tmp

