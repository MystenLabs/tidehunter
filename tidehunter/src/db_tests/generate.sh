#!/bin/bash

script_dir=$(dirname "$0")
target="$script_dir"/generated.rs
python3 "$script_dir"/generate.py > "$target"
# Remove extra end of line so that cargo fmt does not change generated file
truncate -s -1 "$target"
