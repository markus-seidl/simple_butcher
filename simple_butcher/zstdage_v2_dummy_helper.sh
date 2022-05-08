#!/usr/bin/env bash

zstd -3 -T0 "$1" --stdout | age -e -i "$2" | tee  >( sha256sum -b > "$3" ) > "$4"
