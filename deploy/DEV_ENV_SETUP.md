# Dev Environment Setup Guide

## Overview

This repository now supports separate development and production environments running on the same server with different subdomains.

## Architecture

- **Production**: `ticker.example.com` (main branch deployments)
- **Development**: `dev.ticker.example.com` (pull request deployments)

### Separate Databases
- Production: `postgresql://screener:password@db:5432/ticker_screener` on port 5432
- Development: `postgresql://screener_dev:dev_password@db:5433/ticker_screener_dev` on port 5433

### Separate Data Volumes
- Production: `../data/postgres/` and `../artifacts/`
- Development: `../data/postgres-dev/` and `../artifacts-dev/`

## Files Added

### 1. `deploy/docker-compose.dev.yml`
Development Docker Compose configuration with:
- Separate PostgreSQL instance on port 5433
- Development environment variables from `.env.dev`
- Isolated data volumes (`postgres-dev`, `artifacts-dev`)
- Same Caddy config (routing via `APP_FQDN` env var)

### 2. `deploy/.env.example.dev`
Example environment file for development. Copy to `.env.dev` and customize:
```bash
cp deploy/.env.example.dev deploy/.env.dev
# Edit .env.dev with your dev subdomain and credentials
```

## GitHub Actions Workflows

### Existing: `deploy.yml` (Production)
**Triggers**: Main/master branch push or manual workflow dispatch
- Deploys only from `main` or `master` branches
- Uses `docker-compose.yml` (production config)
- Uses secrets: `ORACLE_*` (production server)

### New: `deploy-dev.yml` (Development)
**Triggers**: Pull request events (opened, synchronize, reopened)
- Automatically deploys PR branches to dev environment
- Uses `docker-compose.dev.yml`
- Uses secrets: `DEV_*` (dev server)
- Concurrent deployments are cancelled (only latest runs)

## Required GitHub Secrets

### For Production (`deploy.yml`)
```
ORACLE_HOST          - Production server hostname
ORACLE_USER          - SSH username for prod server
ORACLE_SSH_KEY       - SSH private key for prod server
ORACLE_PORT          - SSH port for prod server
ORACLE_APP_DIR       - Application directory on prod server
```

### For Development (`deploy-dev.yml`)
```
DEV_HOST             - Dev server hostname
DEV_USER             - SSH username for dev server
DEV_SSH_KEY          - SSH private key for dev server
DEV_PORT             - SSH port for dev server
DEV_APP_DIR          - Application directory on dev server
```

## Local Development

### Start Dev Environment Locally
```bash
cd deploy
# Create .env.dev from example
cp .env.example.dev .env.dev

# Start dev stack
docker-compose -f docker-compose.dev.yml up -d

# Check status
docker-compose -f docker-compose.dev.yml ps
```

### View Logs
```bash
docker-compose -f docker-compose.dev.yml logs -f web
docker-compose -f docker-compose.dev.yml logs -f db
```

### Stop Dev Environment
```bash
docker-compose -f docker-compose.dev.yml down
```

## Deployment Flow

### For Features (Development)
1. Create feature branch from `main`
2. Push branch to GitHub
3. Create Pull Request
4. GitHub Actions automatically:
   - Runs CI tests
   - Deploys to `dev.ticker.example.com` on pass
   - Updates deployment on every commit

### For Production
1. Create Pull Request
2. Review and approve
3. Merge to `main`
4. GitHub Actions automatically:
   - Runs CI tests
   - Deploys to `ticker.example.com` on pass
   - Or trigger manual deployment via workflow dispatch

## DNS/Reverse Proxy Configuration

Update your reverse proxy/DNS to route:
- `dev.ticker.example.com` → Dev server IP/hostname
- `ticker.example.com` → Production server IP/hostname

Caddy handles the routing internally via the `APP_FQDN` environment variable.

## Next Steps

1. **Add GitHub Secrets**: Configure `DEV_*` and `ORACLE_*` secrets in repository settings
2. **Setup Dev Server**: Mirror production setup with dev directory
3. **Create Workflows**: Copy workflow snippets from this guide (due to permission constraints, manual creation may be needed)
4. **Configure DNS**: Add dev subdomain routing
5. **Test**: Create a test PR to verify dev deployment

## Troubleshooting

### Workflow Permissions Issue
If you encounter permission errors when updating workflows, ensure:
- You have admin/write access to the repository
- Workflows are in `.github/workflows/` directory
- File names don't conflict with existing workflows

### Dev Deployment Not Triggering
Check:
1. GitHub Actions are enabled in repository settings
2. Secrets are correctly configured
3. Branch push/PR triggers are enabled
4. Check Actions tab for workflow run logs

### Database Connection Issues
Verify:
1. `.env.dev` has correct `TICKER_SCREENER_DATABASE_URL`
2. PostgreSQL is healthy: `docker-compose -f docker-compose.dev.yml ps`
3. Port 5433 isn't in use: `lsof -i :5433`
