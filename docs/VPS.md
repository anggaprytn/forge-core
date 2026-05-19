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

## 1. Install Runtime Dependencies

Forge currently assumes:

- Docker daemon is installed and reachable
- Caddy is installed with the admin API enabled on `http://127.0.0.1:2019`
- the `forge` binary is installed on the host

Example binary install from a local release build:

```bash
install -m 0755 target/release/forge /usr/local/bin/forge
```

## 2. Create Host Directories

```bash
useradd --system --home /srv/forge --shell /usr/sbin/nologin forge
mkdir -p /etc/forge /var/lib/forge /srv/forge/sample-http-app
chown -R forge:forge /var/lib/forge /srv/forge
```

`/var/lib/forge` must exist before startup. Forge bootstrap waits when the configured storage root is missing.

## 3. Install the Sample App

The repository already includes a minimal Docker-backed sample app image definition:

```bash
cp tests/fixtures/sample-http-app/Dockerfile /srv/forge/sample-http-app/Dockerfile
```

Create `/srv/forge/sample-http-app/forge.project.json`:

```json
{
  "forge_schema_version": 1,
  "project_id": "api",
  "repository": { "provider": "github" },
  "environments": {
    "development": { "branch": "dev" },
    "staging": { "branch": "staging" },
    "production": { "branch": "main" }
  },
  "build": { "dockerfile_path": "./Dockerfile", "context_path": "." },
  "runtime": {
    "service_type": "http",
    "internal_port": 3000,
    "subdomain": "api",
    "resources": { "memory_limit_mb": 512, "cpu_shares": 1024 }
  },
  "health": {
    "tcp_required": true,
    "http": { "enabled": true, "path": "/health", "expected_status": [200], "timeout_ms": 5000 },
    "startup_grace_seconds": 30
  },
  "contract": { "version": 1, "spec": {} }
}
```

## 4. Install Forge Config

Copy the example config:

```bash
install -m 0644 examples/forge.conf /etc/forge/forge.conf
```

Then update `/etc/forge/forge.conf` with production values:

- set `bearer_token` to a long random token
- keep `storage_root=/var/lib/forge` unless you have a different data path
- add `repository_cache_root` only if you will use GitHub webhook deploys

## 5. Install Forge Environment Variables

Create `/etc/forge/forge.env`:

```bash
FORGE_MASTER_KEY=<64 hex characters>
FORGE_CADDY_ADMIN_URL=http://127.0.0.1:2019
FORGE_CADDY_PUBLIC_URL=http://127.0.0.1
```

`FORGE_MASTER_KEY` is required for secrets support and checked by `forge doctor`.

## 6. Install systemd Unit

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

## 7. Verify Readiness

Use the same config and Caddy URL the service uses:

```bash
FORGE_CONFIG=/etc/forge/forge.conf \
FORGE_CADDY_ADMIN_URL=http://127.0.0.1:2019 \
FORGE_MASTER_KEY=<64 hex characters> \
forge doctor
```

You should also be able to confirm the API surface directly:

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
curl http://127.0.0.1:8080/metrics
```

## 8. Deploy the Sample App

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
