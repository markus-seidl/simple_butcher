#!/usr/bin/env bash

set -xe

git pull --rebase

source venv/bin/activate

python3 backup.py $*

