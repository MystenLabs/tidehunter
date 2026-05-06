#!/bin/bash
# Print (or optionally execute) the commands needed to wipe R6 experiment DBs
# on benchmark hosts.
#
# The R6 generator places fills under /opt/sui/db/r6/<name>/, deliberately
# outside the orchestrator's <working_dir>/stress.* cleanup pattern so fills
# survive between batches. The downside: the orchestrator never cleans them.
# Measure entries with --clean-after-measure delete their own DB after
# measurement, but aborted/incomplete runs leave residue.
#
# By default this script just *prints* the commands — review them, then run
# them yourself. Pass --execute to run via ssh.
#
# Usage:
#   ./scripts/r6_cleanup.sh                         # dry-run, prints commands
#   ./scripts/r6_cleanup.sh --hosts host1,host2     # custom host list
#   ./scripts/r6_cleanup.sh --hosts ... --execute   # actually run

set -euo pipefail

HOSTS=""
EXECUTE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --hosts) HOSTS="$2"; shift 2 ;;
        --execute) EXECUTE=1; shift ;;
        -h|--help)
            sed -n '2,17p' "$0"
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [ -z "$HOSTS" ]; then
    echo "No hosts provided. Pass --hosts host1,host2,... (comma-separated)." >&2
    echo "Hosts can be read from orchestrator/assets/settings.yml." >&2
    exit 1
fi

IFS=',' read -ra HOST_ARR <<< "$HOSTS"

for h in "${HOST_ARR[@]}"; do
    cmd="ssh $h 'rm -rf /opt/sui/db/r6'"
    if [ "$EXECUTE" -eq 1 ]; then
        echo "+ $cmd"
        eval "$cmd"
    else
        echo "$cmd"
    fi
done

if [ "$EXECUTE" -eq 0 ]; then
    echo
    echo "(dry-run — re-run with --execute to actually wipe)" >&2
fi
