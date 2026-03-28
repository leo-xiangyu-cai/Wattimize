---
name: frontend-render-check
description: Verify the Wattimize frontend by starting the local FastAPI app if needed, opening the dashboard in headless Chromium, capturing a screenshot, and checking browser console or request failures. Use when the user asks to inspect rendered UI, confirm layout changes, compare before/after frontend results, or validate that a page actually renders instead of only reading source code.
---

# Frontend Render Check

Run the real Wattimize UI locally and inspect what the browser actually renders.

Prefer this skill over static code inspection when the task depends on spacing, alignment, clipping, missing data states, tab content, or whether the page loads without frontend errors.

## Workflow

1. Work from the repository root.
2. Run `skills/frontend-render-check/scripts/check_render.sh`.
3. Open the generated screenshot and inspect the rendered result, not just the HTML/CSS source.
4. If the user asked for layout changes, make edits and rerun the script until the screenshot matches the requested outcome.
5. Report both visual findings and runtime issues from the script output.

## Use The Script

Default usage:

```bash
skills/frontend-render-check/scripts/check_render.sh
```

Custom page URL or screenshot path:

```bash
skills/frontend-render-check/scripts/check_render.sh \
  "http://127.0.0.1:18000/#dashboard" \
  "/tmp/wattimize-dashboard.png"
```

The script will:

- Start `uvicorn` on `127.0.0.1:18000` if the app is not already running
- Wait for `/api/health`
- Bootstrap a temporary Playwright environment under `/tmp/wattimize-playwright` if needed
- Install Chromium if needed
- Capture a full-page screenshot
- Print JSON with:
  - `url`
  - `title`
  - `screenshot`
  - `consoleErrors`
  - `pageErrors`
  - `failedRequests`
- Stop the temporary `uvicorn` process if the script started it

## Inspect The Result

After the script finishes:

- Open the screenshot with the image viewer tool.
- Compare visual output against the user request.
- Call out clipping, overlap, excessive empty space, unreadable labels, or missing sections.
- If there are no runtime errors but the UI still looks wrong, treat it as a layout bug rather than a startup failure.
- If runtime errors exist, report them before discussing styling polish.

## Project-Specific Notes

- Wattimize serves the frontend from FastAPI static files. There is no separate frontend dev server.
- The expected local command is `.venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 18000`.
- Live dashboard content depends on current backend/config state. Missing live data is acceptable if the page shell renders correctly; report that distinction clearly.
- When validating a layout tweak, prefer full-page screenshots so the energy-flow board and the next section below it are visible together.

## Resource

Use `scripts/check_render.sh` for the repeatable browser workflow.
