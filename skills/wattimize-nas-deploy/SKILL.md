---
name: wattimize-nas-deploy
description: Check or deploy the Wattimize app against the TerraMaster NAS live runtime while preserving NAS data. Use in /Users/caixy/Leo/Wattimize when the user asks to inspect issues, verify behavior, deploy, publish, push code, refresh the server, or run Wattimize.
---

# Wattimize NAS Runtime

Use this skill for Wattimize runtime checks and deployments from `/Users/caixy/Leo/Wattimize`.

The normal target is the TerraMaster NAS, not a local host-side server or local Docker container. When the user asks to check a problem, verify a fix, inspect live behavior, deploy, publish, push code, refresh the server, or run Wattimize, operate against the NAS unless the user explicitly asks for a local-only workflow.

## NAS-First Rule

For runtime inspection, use the live NAS service:

```bash
http://tnas.local:18000
```

Prefer read-only NAS checks such as:

```bash
curl -fsS --max-time 5 http://tnas.local:18000/api/health
curl -fsS --max-time 8 http://tnas.local:18000/api/config/status
curl -fsS --max-time 8 http://tnas.local:18000/api/collector/status
```

For UI checks, open or test `http://tnas.local:18000` with the browser/Playwright tooling. Do not inspect or start the local Wattimize container just to check normal behavior. Use local static checks for code correctness, but treat NAS as the runtime source of truth.

Only use local runtime checks when the user explicitly asks for local-only verification or the NAS is unreachable and the user chooses a local fallback.

## Data Safety Rule

NAS database continuity is the hard requirement for every deployment.

The NAS container is normally the active writer. Preserve the NAS database and uploaded bill artifacts during ordinary code deploys. Do not overwrite them with local files.

The NAS container writes live data under `${NAS_DATA_DIR}`. Do not overwrite that live database with the local database during ordinary deploys. The local database may be stale because the NAS has been running and collecting samples while local has not.

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

## Required Deploy After Local Changes

For Wattimize tasks that modify source code, static frontend files, app-served documentation/data, tests, skills, or deployment scripts, perform the normal safe deploy before the final response unless the user explicitly says not to deploy. This applies even when the user only asks to "change", "fix", "add", or "update" and does not separately ask to deploy.

Do not deploy for read-only debugging, inspection, or explanation tasks where no files were changed. Updates solely to agent workflow instructions or skill documentation do not require a runtime deployment; validate those files locally. The deployment requirement above applies when changes affect the app, its runtime assets, or its deployment behavior.

If deployment fails at the script's built-in health-check stage, immediately perform direct read-only NAS verification:

```bash
curl -fsS --max-time 5 http://tnas.local:18000/api/health
curl -fsS --max-time 8 http://tnas.local:18000/api/collector/status
```

If these follow-up checks pass and the expected feature is observable, report the deployment as successful with a note that the script's initial health wait timed out during startup. If follow-up checks fail, inspect NAS container status/logs read-only and either fix the local change and redeploy or report the blocker.

## Preflight

1. Work from `/Users/caixy/Leo/Wattimize`.
2. Check local changes with `git status --short`.
3. Confirm the intended code changes are present.
4. Do not start or inspect local services for normal NAS-first workflows.
5. Confirm the deploy path preserves NAS database and uploaded bill artifacts.
6. Run normal static checks when relevant, such as:

```bash
PYTHONPYCACHEPREFIX=/tmp/wattimize-pycache python3 -m py_compile app/main.py app/persistence.py
```

For frontend-only changes, use:

```bash
node --check app/static/app.js
git diff --check
```

## Deployment

Run:

```bash
make nas-deploy SKIP_DB_PUSH=1
```

If sandbox/network/SSH restrictions block it, request escalation for that exact command and explain that it preserves NAS live data by skipping DB push.

## Post-Deploy Verification

After deploy completes, verify NAS, not local:

```bash
curl -fsS --max-time 5 http://tnas.local:18000/api/health
curl -fsS --max-time 8 http://tnas.local:18000/api/config/status
curl -fsS --max-time 8 http://tnas.local:18000/api/collector/status
```

For feature-specific changes, query the relevant read-only endpoint and report the live decision/status.

For browser-facing changes, verify the deployed page at `http://tnas.local:18000` after deployment.

## Dangerous Data Operations

Only do these in a separate data-migration workflow after explaining data-loss risk and confirming the data direction:

- copying local `data/wattimize.sqlite3` to NAS
- importing a database into NAS through API
- restoring a backup over the NAS database
- stopping NAS and replacing `${NAS_DATA_DIR}` contents

For those tasks, first make or confirm a backup and ensure the user understands that live NAS samples since the local copy may be lost.
