#!/bin/bash

script_dir=$(dirname "$0")
python3 "$script_dir"/generate.py > "$script_dir"/generated.rs
