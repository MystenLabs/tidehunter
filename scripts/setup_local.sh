#!/bin/bash

# Sets up this machine so the orchestrator can run benchmarks on it locally
# (single-machine testbed over ssh to 127.0.0.1).
#
# Usage: ./scripts/setup_local.sh
#
# The script is idempotent: it skips anything already done.

set -euo pipefail

# Run from the repository root regardless of where the script is invoked.
cd "$(dirname "$0")/.."
if [ ! -f Cargo.toml ]; then
    echo "Error: could not locate the repository root (no Cargo.toml found)." >&2
    exit 1
fi

SSH_KEY="$HOME/.ssh/tidehunter_local"

echo "===================================================="
echo "1. Installing packages (build essentials, clang, ssh server, font tools)"
echo "===================================================="
if command -v apt-get &> /dev/null; then
    sudo apt-get update -y
    sudo apt-get install -y build-essential clang libclang-dev curl pkg-config \
        libfontconfig1-dev libssl-dev openssh-server
else
    echo "apt-get not found (not an Ubuntu/Debian machine)."
    echo "Please install equivalents of: build-essential clang libclang-dev"
    echo "curl pkg-config libfontconfig1-dev libssl-dev openssh-server"
fi

echo "===================================================="
echo "2. Installing Rust toolchain"
echo "===================================================="
if ! command -v rustc &> /dev/null && [ ! -x "$HOME/.cargo/bin/rustc" ]; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    echo "Rust installed successfully!"
else
    echo "Rust is already installed."
fi
# Make cargo available to the rest of this script.
[ -f "$HOME/.cargo/env" ] && source "$HOME/.cargo/env"

echo "===================================================="
echo "3. Applying permanent CXXFLAGS fix for RocksDB"
echo "===================================================="
if ! grep -q "include cstdint" "$HOME/.bashrc" 2>/dev/null; then
    echo 'export CXXFLAGS="$CXXFLAGS -include cstdint"' >> "$HOME/.bashrc"
    source ~/.bashrc
    echo "RocksDB compilation fix appended to ~/.bashrc"
else
    echo "RocksDB compilation fix already exists in ~/.bashrc"
fi

echo "===================================================="
echo "4. Adding orchestrator crate to the cargo workspace"
echo "===================================================="
if grep -q '"orchestrator"' Cargo.toml; then
    echo "Orchestrator is already present in Cargo.toml workspace members."
else
    sed -i '/members *= *\[/a \    "orchestrator",' Cargo.toml
    echo "Added \"orchestrator\" to the Cargo.toml workspace members."
fi

echo "===================================================="
echo "5. Ensuring the ssh server is running"
echo "===================================================="
# The orchestrator drives all machines over ssh, including the local one.
if command -v systemctl &> /dev/null; then
    if systemctl is-active --quiet ssh || systemctl is-active --quiet sshd; then
        echo "ssh server is running."
    else
        echo "Starting ssh server..."
        sudo systemctl enable --now ssh 2>/dev/null || sudo systemctl enable --now sshd
    fi
else
    echo "systemctl not found. Please make sure an ssh server is running on port 22."
    echo "(On macOS: System Settings -> General -> Sharing -> Remote Login.)"
fi

echo "===================================================="
echo "6. Creating a dedicated ssh key for the local testbed"
echo "===================================================="
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
if [ ! -f "$SSH_KEY" ]; then
    ssh-keygen -t ed25519 -f "$SSH_KEY" -N "" -C "tidehunter-local-testbed"
    echo "Created $SSH_KEY"
else
    echo "$SSH_KEY already exists."
fi

touch "$HOME/.ssh/authorized_keys"
chmod 600 "$HOME/.ssh/authorized_keys"
if grep -qxFf "$SSH_KEY.pub" "$HOME/.ssh/authorized_keys"; then
    echo "Public key already authorized."
else
    cat "$SSH_KEY.pub" >> "$HOME/.ssh/authorized_keys"
    echo "Public key added to ~/.ssh/authorized_keys."
fi

echo "===================================================="
echo "7. Verifying this machine can ssh into itself"
echo "===================================================="
if ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
        -o ConnectTimeout=5 "$USER@127.0.0.1" true; then
    echo "Self-ssh works."
else
    echo "Error: could not ssh to $USER@127.0.0.1 with $SSH_KEY." >&2
    echo "Check that the ssh server is running and allows public key authentication." >&2
    exit 1
fi

echo "===================================================="
echo "Setup complete!"
echo "===================================================="

