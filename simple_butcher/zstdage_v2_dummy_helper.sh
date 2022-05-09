#!/usr/bin/env bash

zstd -3 -T0 "$1" --stdout | age -e -i "$2" | tee  >( md5sum -b > "$3" ) > "$4"
