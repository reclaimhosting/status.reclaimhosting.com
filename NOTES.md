# status.reclaimhosting.com -> GH issue sync script

A Python script that:

- Reads active/upcoming maintenance + incidents from Uptime Kuma 2.x using MySQL or MariaDB.
- Creates/updates GitHub issues for each active event.
- Automatically closes managed issues when the event is no longer active.
- Manages monitors on an Uptime Kuma status page:
  - Add all monitors to the specified page/group
  - Remove paused monitors
  - Sort them alphabetically
  - Exclude monitors with tags from `IGNORE_TAGS`

Refer to [`env.example`](env.example) for all configuration information and options

## Local testing

Use [`.env.example`](env.example) as the starting point for local testing:

```bash
cp .env.example .env
```

Build and run the container:

```bash
docker build -t uptime-kuma-status-page-utils . && docker run --rm --env-file .env uptime-kuma-status-page-utils
```

## To Do
- [ ] Set up api token not tied to Taylor's github profile
- [ ] Review all automated message templates
  - Remove "status" boilerplate. consider including links back to status.reclaimhosting.com for all updates
- [ ] Make maintenance issues close when the window is over
- [x] Create a README with instructions for subscribing
- [ ] Set up on DO
- [x] Link to this from status.reclaimhosting.com
