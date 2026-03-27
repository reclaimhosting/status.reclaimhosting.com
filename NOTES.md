# Uptime Kuma → GitHub Incident Sync

A lightweight Python worker for that:

1. Reads active/upcoming maintenance + incidents from Uptime Kuma 2.x using MySQL or MariaDB.
2. Creates/updates GitHub issues for each active event.
3. Automatically closes managed issues when the event is no longer active.
4. Optionally keeps an Uptime Kuma status page in sync with all non-paused monitors, excluding monitors with ignored tags.

## Environment Variables

### Required

GitHub:
- `GITHUB_REPO` - `owner/repo`
- `GITHUB_TOKEN` - token with `repo` (private) or `public_repo` (public)

Database:
- `UPTIME_KUMA_DB_HOSTNAME`
- `UPTIME_KUMA_DB_PORT` (default: `3306`)
- `UPTIME_KUMA_DB_USERNAME`
- `UPTIME_KUMA_DB_PASSWORD`
- `UPTIME_KUMA_DB_NAME`

### Optional

- `POLL_INTERVAL_SECONDS` (default: `120`)
- `RUN_ONCE` (`true`/`false`, default: `false`)
- `LOG_LEVEL` (default: `INFO`)
- `GITHUB_LABEL` (default: `uptime-kuma`)
- `AUTO_CLOSE_RESOLVED` (`true`/`false`, default: `true`)
- `MANAGE_STATUS_PAGE` (`true`/`false`, default: `false`; if `true`, syncs all non-paused monitors to the status page group)
- `IGNORE_TAGS` (optional comma-separated tag names; monitors with any of these tags are excluded from status-page sync)
- `STATUS_PAGE_SLUG` (default: `default`)
- `STATUS_PAGE_GROUP_NAME` (default: `Services`, group used/created for status-page-synced monitors)
- `STATUS_PAGE_SORT_ALPHABETIC` (`true`/`false`, default: `false`; if `true`, reorders managed monitors alphabetically by monitor name)

## Local testing

Use [`.env.example`](./.env.example) as the starting point for local testing:

```bash
cp .env.example .env
```

Build and run the container:

```bash
docker build -t uptime-kuma-status-page-utils . && docker run --rm --env-file .env uptime-kuma-status-page-utils
```

## Further Notes

- The worker stores hidden metadata in issue bodies so the same Kuma event is updated instead of duplicated.
- Auto-close only applies to open issues with the managed label (default: `uptime-kuma`).
- Maintenance issues include linked monitor names under a section titled from `STATUS_PAGE_GROUP_NAME` when affected monitors are available.
- If `MANAGE_STATUS_PAGE=true`, the worker syncs the status page group to all non-paused monitors. Monitors with tags from `IGNORE_TAGS` are excluded.
- Status-page sync adds missing managed monitors and removes stale links, including links for paused monitors.
- If `STATUS_PAGE_SORT_ALPHABETIC=true`, the worker also rewrites the group order alphabetically by monitor name.
- Supported scope: Uptime Kuma 2.x with MySQL/MariaDB only.
