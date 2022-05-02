#!/usr/bin/env bash

#git pull --rebase

source venv/bin/activate

__python=$(which python3)

$__python simple_butcher "$@"
