# Uptime Kuma Status Page util

A Python script that:

1. Reads active/upcoming maintenance + incidents from Uptime Kuma 2.x using MySQL or MariaDB.
2. Creates/updates GitHub issues for each active event.
3. Automatically closes managed issues when the event is no longer active.
4. Optionally keeps an Uptime Kuma status page in sync with all non-paused monitors, excluding monitors with ignored tags.


## More info
- Refer to [`env.example`](env.example) for configuration information and options
- `MANAGE_STATUS_PAGE` sync options automatically manages monitors on a specific page in a spefic group:
  - Add all monitors to the page/group
  - Remove paused monitors
  - Sort them alphabetically
  - Monitors with tags from `IGNORE_TAGS` can be excluded

## Local testing

Use [`.env.example`](env.example) as the starting point for local testing:

```bash
cp .env.example .env
```

Build and run the container:

```bash
docker build -t uptime-kuma-status-page-utils . && docker run --rm --env-file .env uptime-kuma-status-page-utils
```
