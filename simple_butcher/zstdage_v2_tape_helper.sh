#!/usr/bin/env bash

zstd -4 -T0 "$1" --stdout | age -e -i "$2" | tee >( mbuffer -P 90 -l "$3" -q -o "$4" -m 5G -s "512k" ) >( md5sum -b > "$5" ) > /dev/null
