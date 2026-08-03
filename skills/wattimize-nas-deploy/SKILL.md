---
name: wattimize-nas-deploy
description: Deploy Wattimize code without losing live database data. Use when the user asks to deploy, publish, push code, refresh the NAS/server/local runtime, or run any deployment for this Wattimize project.
---

# Wattimize NAS Deploy

Use this skill for deploying code from this repository to the TerraMaster NAS or any other runtime.

## Data Safety Rule

Database continuity is the hard requirement for every deployment.

Before any deploy, identify which runtime has been actively writing the database. Preserve that live database and never overwrite it with an older or inactive copy.

The NAS container writes live data under `${NAS_DATA_DIR}`. Do not overwrite that live database with the local database during ordinary deploys. The local database may be stale because the NAS has been running and collecting samples while local has not.

The same principle applies in the other direction: do not overwrite a newer local database with a stale remote copy if local has been the active writer.

Never run these during a code deploy:

```bash
scripts/push-db-to-nas.sh
make nas-deploy
make nas-deploy NO_DB_PUSH=0
make nas-deploy SKIP_DB_PUSH=0
```

Only perform database copy/restore/import/export as a separate data-migration task after:

- identifying source and destination freshness
- stopping all writers that can touch the destination database
- taking a backup of the destination database
- explicitly confirming the intended data direction with the user

## Normal Safe Deploy

Use:

```bash
make nas-deploy SKIP_DB_PUSH=1
```

This preserves the NAS SQLite database and uploaded bill artifacts. The existing deploy script may still sync `data/home-plan/electricity-plans.json`; that is acceptable for normal code deploys because it is not the live sample/history database.

Before running, state that the command:

- builds a fresh Docker image from local code
- uploads the image TAR to the NAS
- restarts the NAS container
- skips local SQLite and bill artifact push
- preserves NAS live runtime data

## Preflight

1. Work from `/Users/caixy/Leo/Wattimize`.
2. Check local changes with `git status --short`.
3. Confirm the intended code changes are present.
4. Do not start local services.
5. Confirm the deploy path does not replace the active database with a stale copy.
6. Run normal static checks when relevant, such as:

```bash
PYTHONPYCACHEPREFIX=/tmp/wattimize-pycache python3 -m py_compile app/main.py app/persistence.py
```

## Deployment

Run:

```bash
make nas-deploy SKIP_DB_PUSH=1
```

If sandbox/network/SSH restrictions block it, request escalation for that exact command and explain that it preserves NAS live data by skipping DB push.

## Post-Deploy Verification

After deploy completes:

```bash
curl -fsS --max-time 5 http://tnas.local:18000/api/health
curl -fsS --max-time 8 http://tnas.local:18000/api/config/status
curl -fsS --max-time 8 http://tnas.local:18000/api/collector/status
```

For feature-specific changes, query the relevant read-only endpoint and report the live decision/status.

## Dangerous Data Operations

Only do these in a separate data-migration workflow after explaining data-loss risk and confirming the data direction:

- copying local `data/wattimize.sqlite3` to NAS
- importing a database into NAS through API
- restoring a backup over the NAS database
- stopping NAS and replacing `${NAS_DATA_DIR}` contents

For those tasks, first make or confirm a backup and ensure the user understands that live NAS samples since the local copy may be lost.
