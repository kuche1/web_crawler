#! /usr/bin/env bash

set -euo pipefail

mypy --strict crawler.py
mypy --strict search.py
