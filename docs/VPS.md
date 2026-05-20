# Forge VPS Guide

This guide documents the current single-node operator path:

```txt
forge daemon
→ HTTP API
→ CLI/API deploy flow
```

It is intentionally aligned to the current implementation, not an aspirational installer.

## Important Current Constraint

Manual `forge deploy <project_id> <environment>` deployments build from the Forge daemon process working directory.

For a fresh VPS sample deployment, point the systemd unit `WorkingDirectory` at the sample app checkout you want Forge to build.

GitHub webhook deployments do not rely on the daemon working directory, but they do require `repository_cache_root` and webhook configuration.

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
chown -R forge:forge /var/lib/forge /srv/forge
```

`/var/lib/forge` must exist before startup. Forge bootstrap waits when the configured storage root is missing.

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

This generates `forge.yml`. This is the primary operator-facing configuration for Forge.

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

## 11. Verify Readiness

You should also be able to confirm the API surface directly:

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
curl http://127.0.0.1:8080/metrics
```

## 12. Deploy the Sample App

Manual deploys go through the HTTP API. Set the CLI client environment first:

```bash
export FORGE_URL=http://127.0.0.1:8080
export FORGE_TOKEN=replace-with-the-bearer_token-from-forge.conf
```

Then enqueue the deploy:

```bash
forge deploy api production
forge events
```

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
