# Forge VPS Guide

This guide documents the current single-node operator path:

```txt
forge daemon
→ HTTP API
→ CLI/API deploy flow
```

It is intentionally aligned to the current implementation, not an aspirational installer.

## 1. Prerequisites (Docker & Caddy)

Forge does not install Docker or Caddy for you. Install them using your distribution's package manager.

### Install Docker

```bash
apt-get update
apt-get install -y docker.io
systemctl enable --now docker
```

### Install Caddy

```bash
apt-get update
apt-get install -y caddy
systemctl enable --now caddy
```

### Configure Caddy Admin API

Forge expects the Caddy admin API at `http://127.0.0.1:2019`.

Example `/etc/caddy/Caddyfile`:

```caddyfile
{
	admin 127.0.0.1:2019
}

:80 {
	respond "caddy ready" 200
}
```

Restart Caddy: `systemctl restart caddy`.

---

## 2. Conservative Installation

For Linux hosts with systemd, use the provided conservative installer:

```bash
./install.sh
```

The installer is **idempotent** and safe:
- Installs `forge` to `/usr/local/bin`.
- Creates `/etc/forge/forge.conf` and `/etc/forge/forge.env` if missing.
- Prepares `/var/lib/forge` for storage.
- Installs the systemd unit `forge.service`.

---

## 3. Host Directory & Permissions

While `install.sh` creates the storage root, you must ensure your application checkout is accessible:

```bash
# Example project directory
mkdir -p /srv/forge/sample-http-app
chown -R forge:forge /var/lib/forge
```

### Critical Permission Rules
- **Storage**: `/var/lib/forge` (storage_root) MUST be owned by the `forge` service user.
- **Project**: The `WorkingDirectory` (e.g., `/srv/forge/sample-http-app`) must be readable and traversable by the `forge` service user.

---

## 4. Initialize Your Project

Go to your application directory and initialize `forge.yml`:

```bash
cd /srv/forge/sample-http-app
forge init
```

Forge strictly validates `forge.yml`. Unsupported fields are rejected.

---

## 5. Configure Forge Environment

Update `/etc/forge/forge.env` with your master key:

```bash
FORGE_MASTER_KEY=<64 hex characters>
FORGE_CADDY_ADMIN_URL=http://127.0.0.1:2019
FORGE_CADDY_PUBLIC_URL=https://api.forge.example.com
```

`FORGE_MASTER_KEY` is required for secrets support.

Forge web login is part of the human operator control surface and requires these env vars:

```bash
FORGE_GITHUB_OAUTH_CLIENT_ID=...
FORGE_GITHUB_OAUTH_CLIENT_SECRET=...
FORGE_PUBLIC_URL=https://forge.example.com
FORGE_SESSION_SECRET=...
FORGE_CLI_TOKEN_SECRET=...
```

`FORGE_PUBLIC_URL` must be the public Forge origin used by operators and `forge login`.
`FORGE_CLI_TOKEN_SECRET` signs CLI bearer tokens issued after browser approval.

`/login` starts the GitHub OAuth flow, `/app` requires the resulting session cookie, `/login/cli?code=...` serves the CLI approval page, and `/api/cli-login/*` drives the short-lived browser approval flow used by `forge login`.
The static control-plane assets under `web/` are served by Forge itself, so the same process remains authoritative for auth, session validation, and protected page delivery.
CLI commands and bearer-token API auth remain available for automation and operator usage. Web actions are not a separate deployment engine; they flow through the same API and queue.

---

## 6. Run Diagnostics

Before starting the service, verify your environment:

```bash
FORGE_CONFIG=/etc/forge/forge.conf \
FORGE_CADDY_ADMIN_URL=http://127.0.0.1:2019 \
FORGE_MASTER_KEY=<64 hex characters> \
forge doctor
```

---

## 7. Start the Forge Daemon

```bash
systemctl daemon-reload
systemctl enable --now forge
```

### Manual Deployment Note
By default, manual `forge deploy <project> <environment>` deployments build from the Forge daemon process `WorkingDirectory`. Prefer `forge deploy --from <path> <project> <environment>` when you want to target an explicit checkout. `--from` remains an alpha/dev-mode operator path; long-term canonical deploy source is `repository + ref`, resolved to an immutable local checkout.

---

## 8. Verify Readiness

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
curl http://127.0.0.1:8080/metrics
```

---

## 9. Deploy the Sample App

Set the CLI client environment for bearer-token auth, or use browser approval:

```bash
export FORGE_URL=http://127.0.0.1:8080
export FORGE_TOKEN=replace-with-the-bearer_token-from-forge.conf
forge login https://forge.example.com
```

Enqueue the deploy:

```bash
forge deploy api production
forge deploy api production --from /srv/forge/sample-http-app
forge events
```

Cleanup and orphan recovery outcomes are emitted into the same event stream:

```bash
forge events | rg 'ORPHANED_|CLEANUP_'
```

---

## Troubleshooting VPS Deployments

- **Caddy server ID**: Ensure Caddy is configured with server ID `"forge"`.
- **Port Conflicts**: If port 8080 is taken, update `api_bind` in `forge.conf` and `FORGE_URL`.
- **API Visibility**: Keep the API bound to `localhost` (127.0.0.1) for security.

## 1. Install Docker

This guide uses Debian or Ubuntu package names. If your VPS uses another distro, install equivalent packages and keep the same service names and paths.

```bash
apt-get update
apt-get install -y docker.io
systemctl enable --now docker
```

Confirm Docker is available:

```bash
docker version
```

## 2. Install Caddy

```bash
apt-get update
apt-get install -y caddy
systemctl enable --now caddy
```

## 3. Configure the Caddy Admin API

Forge expects the Caddy admin API at `http://127.0.0.1:2019`.

Example `/etc/caddy/Caddyfile`:

```caddyfile
{
	admin 127.0.0.1:2019
}

:80 {
	respond "caddy ready" 200
}
```

Restart Caddy after updating the config:

```bash
systemctl restart caddy
curl http://127.0.0.1:2019/config/
```

Forge only manages its dedicated subtree through the admin API. Do not disable the admin listener.

## 4. Install Runtime Dependencies

Forge currently assumes:

- Docker daemon is installed and reachable
- Caddy is installed with the admin API enabled on `http://127.0.0.1:2019`
- the `forge` binary is installed on the host

Example binary install from a local release build:

```bash
install -m 0755 target/release/forge /usr/local/bin/forge
```

## 5. Create Host Directories

```bash
useradd --system --home /srv/forge --shell /usr/sbin/nologin forge
mkdir -p /etc/forge /var/lib/forge /srv/forge/sample-http-app
chown -R forge:forge /var/lib/forge
```

`/var/lib/forge` must exist before startup. Forge bootstrap waits when the configured storage root is missing.

Forge only requires the service user to own `storage_root`. The project checkout named by the systemd `WorkingDirectory` only needs to be readable and searchable by that same service user.

## 6. Install the Sample App

The repository already includes a minimal Docker-backed sample app image definition:

```bash
cp tests/fixtures/sample-http-app/Dockerfile /srv/forge/sample-http-app/Dockerfile
```

Initialize the project configuration using `forge init`:

```bash
cd /srv/forge/sample-http-app
forge init
```

This generates `forge.yml`. This is the current alpha operator-facing configuration for Forge.

### Example forge.yml

Forge strictly validates `forge.yml`. Unsupported or unknown fields are rejected intentionally.

```yaml
version: 1
name: api
type: web # Only single-service web apps supported currently

build:
  dockerfile: Dockerfile
  context: .

runtime:
  port: 3000
  healthcheck:
    path: /health
    expected_status: 200

invariants:
  - name: health
    path: /health
    expect_status: 200
```

## 7. Configure `forge.conf`

Copy the example config:

```bash
install -m 0644 deploy/forge.conf.example /etc/forge/forge.conf
```

Then update `/etc/forge/forge.conf` with production values:

- set `bearer_token` to a long random token
- keep `storage_root=/var/lib/forge` unless you have a different data path
- add `repository_cache_root` only if you will use GitHub webhook deploys
- keep `api_bind=127.0.0.1:8080` unless you intentionally want the API bound elsewhere

## 8. Install Forge Environment Variables

Create `/etc/forge/forge.env`:

```bash
FORGE_MASTER_KEY=<64 hex characters>
FORGE_CADDY_ADMIN_URL=http://127.0.0.1:2019
FORGE_CADDY_PUBLIC_URL=https://api.forge.example.com
```

`FORGE_MASTER_KEY` is required for secrets support and checked by `forge doctor`.
`FORGE_CADDY_PUBLIC_URL` should point to the public entrypoint for route validation.

## 9. Run `forge doctor`

Use the same config and Caddy URL the service will use:

```bash
FORGE_CONFIG=/etc/forge/forge.conf \
FORGE_CADDY_ADMIN_URL=http://127.0.0.1:2019 \
FORGE_MASTER_KEY=<64 hex characters> \
forge doctor
```

Expected checks include:

- Docker reachable
- Caddy admin API reachable
- storage root exists and is writable
- `FORGE_MASTER_KEY` present

## 10. Start the Forge Daemon with systemd

```bash
install -D -m 0644 deploy/forge.service /etc/systemd/system/forge.service
systemctl daemon-reload
systemctl enable forge
systemctl start forge
```

The provided unit sets:

- `ExecStart=/usr/local/bin/forge --config /etc/forge/forge.conf daemon`
- `EnvironmentFile=-/etc/forge/forge.env`
- `WorkingDirectory=/srv/forge/sample-http-app`

If you move the sample app checkout, update the unit `WorkingDirectory` to match.

### Service User and Permissions

Forge daemon file permissions follow the effective systemd service account:

- `/var/lib/forge` must be owned by the service `User` so the daemon can persist queue, events, secrets, and deployment state.
- The `WorkingDirectory` must be readable and executable by that same `User`.
- Forge does not need the installer to recursively take ownership of your application checkout. Keep that change explicit and operator-controlled.

If you preserve an existing unit or drop-in override, check the effective account and storage ownership before starting the daemon:

```bash
SERVICE_USER="$(systemctl show --property User --value forge.service)"
[ -n "$SERVICE_USER" ] || SERVICE_USER=root
SERVICE_GROUP="$(systemctl show --property Group --value forge.service)"
[ -n "$SERVICE_GROUP" ] || SERVICE_GROUP="$(id -gn "$SERVICE_USER")"
WORKDIR="$(systemctl show --property WorkingDirectory --value forge.service)"

chown -R "$SERVICE_USER:$SERVICE_GROUP" /var/lib/forge
sudo -u "$SERVICE_USER" test -r "$WORKDIR" && sudo -u "$SERVICE_USER" test -x "$WORKDIR"
```

If the `test` command fails, fix the project checkout permissions deliberately for that directory before enabling the service.

## 11. Verify Readiness

You should also be able to confirm the API surface directly:

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
curl http://127.0.0.1:8080/metrics
```

## 12. Deploy the Sample App

Manual deploys go through the HTTP API. Set the CLI client environment first, or log in once with browser approval:

```bash
export FORGE_URL=http://127.0.0.1:8080
export FORGE_TOKEN=replace-with-the-bearer_token-from-forge.conf
forge login https://forge.example.com
```

Then enqueue the deploy:

```bash
forge deploy api production
forge events
```

Cleanup and orphan recovery outcomes are emitted into the same event stream. To inspect them directly:

```bash
forge events | rg 'ORPHANED_|CLEANUP_'
```

Relevant event types:

- `ORPHANED_CONTAINER_REMOVED`
- `ORPHANED_CONTAINER_TOMBSTONED`
- `ORPHANED_ROUTE_REMOVED`
- `ORPHANED_ROUTE_TOMBSTONED`
- `CLEANUP_RETRY_SUCCEEDED`
- `CLEANUP_RETRY_TOMBSTONED`

This is the current VPS-ready flow:

```txt
systemctl start forge
→ forge doctor
→ forge deploy api production
```

## Troubleshooting VPS Deployments

- **Caddy server ID**: Ensure Caddy is configured with server ID `"forge"`.
- **Public Ingress**: If using Nginx as a public ingress, ensure it correctly proxies to the Caddy-managed routes or Forge-managed containers.
- **Port Conflicts**: If port 8080 is taken, update `api_bind` in `forge.conf` and `FORGE_URL`.
- **Docker Network**: Forge-managed containers must be reachable by Caddy.
