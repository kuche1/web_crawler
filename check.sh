#! /usr/bin/env bash

set -euo pipefail

HERE=$(dirname "$BASH_SOURCE")

mypy --strict "$HERE/crawler.py"
mypy --strict "$HERE/search.py"
