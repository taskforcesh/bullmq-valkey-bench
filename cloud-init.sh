#!/bin/bash
# Bootstrap script for EC2 benchmark instances.
# Installs Docker (+ Compose v2), Node.js 24, and signals readiness.
set -euxo pipefail
exec > /var/log/bench-init.log 2>&1

# ── Docker ────────────────────────────────────────────────────────────
dnf install -y docker
systemctl enable --now docker
usermod -aG docker ec2-user

# Ensure ec2-user can use docker immediately (group change
# requires re-login, but SSH sessions won't trigger that)
chmod 666 /var/run/docker.sock

# Docker Compose v2 plugin
COMPOSE_URL="https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$(uname -m)"
mkdir -p /usr/local/lib/docker/cli-plugins
curl -SL "$COMPOSE_URL" -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# ── Node.js 24 (NodeSource) ──────────────────────────────────────────
curl -fsSL https://rpm.nodesource.com/setup_24.x | bash -
dnf install -y nodejs

# ── Signal completion ─────────────────────────────────────────────────
touch /tmp/cloud-init-done
