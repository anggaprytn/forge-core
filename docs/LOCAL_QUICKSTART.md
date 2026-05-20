# Forge Local Quickstart

This guide is the smallest local alpha loop for Forge.

It does not install Docker, Caddy, package managers, or public ingress for you.

## Prerequisites

- Rust and Cargo
- Docker daemon running locally
- Caddy running locally with the admin API enabled on `http://127.0.0.1:2019`
- `curl`

Example minimal Caddy config for local use:

```caddyfile
{
	admin 127.0.0.1:2019
}

:80 {
	respond "caddy ready" 200
}
```

## Important Current Limitation

By default, manual `forge deploy <project> <environment>` builds from the Forge daemon process `WorkingDirectory`.

For explicit operator control, prefer `forge deploy --from <path> <project> <environment>`. If you omit `--from`, start the daemon from the project directory that contains the `Dockerfile` and `forge.yml` you want to deploy. `--from` remains the alpha/dev-mode source path; the long-term canonical source model is `repository + ref`, resolved into an immutable local checkout before deployment runs.

## 1. Build Forge

From the repo root:

```bash
cargo build --release
```

The binary will be available at `./target/release/forge`.

## 2. Create a Local Control-Plane Config

From the repo root:

```bash
mkdir -p .local
cat > .local/forge.conf <<EOF
storage_root=$(pwd)/.local/var/lib/forge
api_bind=127.0.0.1:18080
bearer_token=dev-token
EOF
mkdir -p .local/var/lib/forge
```

Export the local environment Forge expects:

```bash
export FORGE_CONFIG="$(pwd)/.local/forge.conf"
export FORGE_MASTER_KEY="$(openssl rand -hex 32)"
export FORGE_CADDY_ADMIN_URL="http://127.0.0.1:2019"
export FORGE_CADDY_PUBLIC_URL="http://127.0.0.1"
export FORGE_URL="http://127.0.0.1:18080"
export FORGE_TOKEN="dev-token"
```

If `openssl` is unavailable, generate any 64-character hex value another way.

## 3. Initialize a Sample Project

Forge already includes a sample app fixture with a `Dockerfile`:

```bash
cd tests/fixtures/sample-http-app
../../../target/release/forge init
```

`forge init` creates `forge.yml` in the current directory.

## 4. Run Local Diagnostics

From the repo root or any shell that still has the exports above:

```bash
./target/release/forge doctor
```

Expected checks include Docker reachability, Caddy admin API reachability, storage root existence, and `FORGE_MASTER_KEY` presence.

## 5. Start the Forge Daemon

Keep the daemon in the sample app directory so manual deploys build from that directory:

```bash
cd tests/fixtures/sample-http-app
../../../target/release/forge --config "$FORGE_CONFIG" daemon
```

In another terminal, verify the API is up:

```bash
curl http://127.0.0.1:18080/healthz
curl http://127.0.0.1:18080/readyz
curl http://127.0.0.1:18080/metrics
```

## 6. Deploy the Sample App

From `tests/fixtures/sample-http-app` in a second shell:

```bash
../../../target/release/forge deploy api production
../../../target/release/forge deploy api production --from "$(pwd)"
```

The deploy response includes a `deployment_id`. Keep it for status and log lookups.

## 7. Read Events and Logs

Recent events:

```bash
../../../target/release/forge events
```

Deployment logs for one deployment:

```bash
curl \
  -H "Authorization: Bearer $FORGE_TOKEN" \
  "$FORGE_URL/logs/<deployment_id>"
```

If you started the daemon manually, its process output is also useful during local debugging.

## 8. Roll Back

Rollback restores the previous healthy finalized generation:

```bash
../../../target/release/forge rollback api production
```

## Optional Linux Host Install

For a Linux host with systemd, the repo now includes a conservative and idempotent installer:

```bash
./install.sh
```

It installs the binary, creates `/etc/forge/forge.conf` and `/etc/forge/forge.env` if missing, prepares `/var/lib/forge`, and installs the provided systemd unit without enabling it automatically.

### Permissions and Paths
Before `systemctl enable --now forge`, verify the effective service account can write `/var/lib/forge` and can read/traverse the configured `WorkingDirectory`:

```bash
SERVICE_USER="$(systemctl show --property User --value forge.service)"
[ -n "$SERVICE_USER" ] || SERVICE_USER=root
SERVICE_GROUP="$(systemctl show --property Group --value forge.service)"
[ -n "$SERVICE_GROUP" ] || SERVICE_GROUP="$(id -gn "$SERVICE_USER")"
WORKDIR="$(systemctl show --property WorkingDirectory --value forge.service)"

sudo chown -R "$SERVICE_USER:$SERVICE_GROUP" /var/lib/forge
sudo -u "$SERVICE_USER" test -r "$WORKDIR" && sudo -u "$SERVICE_USER" test -x "$WORKDIR"
```

If the `test` command fails, adjust the checkout directory ownership or mode explicitly instead of recursively chowning arbitrary project trees by default.
