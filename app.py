#!/usr/bin/env python3
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import pymysql
import requests

MARKER_PREFIX = "<!-- kuma-event-key:"
MANAGED_LABEL = "uptime-kuma"
logger = logging.getLogger(__name__)


@dataclass
class Event:
    key: str
    kind: str
    title: str
    description: str
    status: str
    start_at: Optional[str]
    end_at: Optional[str]
    affected_services: List[str]


class DBClient:
    def __init__(self):
        self.conn: Any = None

    def __enter__(self):
        self.conn = pymysql.connect(
            host=env("UPTIME_KUMA_DB_HOSTNAME", required=True),
            port=int(env("UPTIME_KUMA_DB_PORT", "3306")),
            user=env("UPTIME_KUMA_DB_USERNAME", required=True),
            password=env("UPTIME_KUMA_DB_PASSWORD", required=True),
            database=env("UPTIME_KUMA_DB_NAME", required=True),
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.conn:
            self.conn.close()

    def list_tables(self) -> List[str]:
        with self.conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            rows = cur.fetchall()
        return [list(r.values())[0] for r in rows]

    def table_columns(self, table: str) -> List[str]:
        with self.conn.cursor() as cur:
            cur.execute(f"SHOW COLUMNS FROM `{table}`")
            rows = cur.fetchall()
        return [r["Field"] for r in rows]

    def fetch_table_rows(self, table: str) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT * FROM `{table}`")
            return cur.fetchall()


def env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def parse_time(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(raw, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def first_existing(columns: List[str], choices: Iterable[str]) -> Optional[str]:
    lookup = {c.lower(): c for c in columns}
    for choice in choices:
        if choice.lower() in lookup:
            return lookup[choice.lower()]
    return None


def normalize_status(row: Dict[str, Any], columns: List[str]) -> str:
    status_col = first_existing(columns, ["status", "state"])
    active_col = first_existing(columns, ["active", "is_active", "enabled"])
    if status_col and row.get(status_col) is not None:
        return str(row[status_col])
    if active_col:
        return "active" if bool(row.get(active_col)) else "inactive"
    return "unknown"


def choose_kind(table: str) -> str:
    t = table.lower()
    if "maintenance" in t:
        return "maintenance"
    if "incident" in t:
        return "incident"
    return "event"


def should_include_event(row: Dict[str, Any], columns: List[str], now: datetime) -> bool:
    status = normalize_status(row, columns).lower()
    if status in {"resolved", "done", "complete", "completed", "inactive", "closed"}:
        return False

    start_col = first_existing(columns, ["start_date", "start_at", "start", "from", "scheduled_for"])
    end_col = first_existing(columns, ["end_date", "end_at", "end", "to", "scheduled_until"])
    active_col = first_existing(columns, ["active", "is_active", "enabled"])

    if active_col and bool(row.get(active_col)):
        return True

    end_dt = parse_time(str(row[end_col])) if end_col and row.get(end_col) is not None else None
    if end_dt and end_dt < now:
        return False

    start_dt = parse_time(str(row[start_col])) if start_col and row.get(start_col) is not None else None
    if start_dt and start_dt > now:
        return True

    return status not in {"unknown"}


def fetch_maintenance_monitors(db: DBClient, maintenance_id: Any) -> List[str]:
    tables = db.list_tables()
    maintenance_monitor_table = find_preferred_table_name(
        tables,
        ["maintenance_monitor"],
        ["maintenance", "monitor"],
        exclude_tokens=["status_page"],
    )
    monitor_table = find_exact_table_name(tables, "monitor")
    if not maintenance_monitor_table or not monitor_table:
        return []

    maintenance_monitor_columns = db.table_columns(maintenance_monitor_table)
    maintenance_id_col = first_existing(maintenance_monitor_columns, ["maintenance_id", "maintenanceId", "maintenance"])
    maintenance_monitor_id_col = first_existing(maintenance_monitor_columns, ["monitor_id", "monitorId", "monitor"])
    if not maintenance_id_col or not maintenance_monitor_id_col:
        return []

    monitor_columns = db.table_columns(monitor_table)
    monitor_id_col = first_existing(monitor_columns, ["id", "monitor_id"])
    monitor_name_col = first_existing(monitor_columns, ["name", "friendly_name"])
    if not monitor_id_col or not monitor_name_col:
        return []

    linked_monitor_ids = {
        row[maintenance_monitor_id_col]
        for row in db.fetch_table_rows(maintenance_monitor_table)
        if row.get(maintenance_id_col) == maintenance_id and row.get(maintenance_monitor_id_col) is not None
    }
    if not linked_monitor_ids:
        return []

    affected_services = [
        str(row.get(monitor_name_col, "")).strip()
        for row in db.fetch_table_rows(monitor_table)
        if row.get(monitor_id_col) in linked_monitor_ids and str(row.get(monitor_name_col, "")).strip()
    ]
    return sorted(set(affected_services), key=str.lower)


def fetch_events_from_table(db: DBClient, table: str, now: datetime) -> List[Event]:
    columns = db.table_columns(table)
    id_col = first_existing(columns, ["id", "incident_id", "maintenance_id"])
    title_col = first_existing(columns, ["title", "name", "subject"])
    description_col = first_existing(columns, ["description", "content", "message", "body"])
    start_col = first_existing(columns, ["start_date", "start_at", "start", "from", "scheduled_for"])
    end_col = first_existing(columns, ["end_date", "end_at", "end", "to", "scheduled_until"])

    if not id_col or not title_col:
        return []

    events: List[Event] = []
    for row in db.fetch_table_rows(table):
        if not should_include_event(row, columns, now):
            continue

        title = str(row.get(title_col, "")).strip()
        if not title:
            continue

        events.append(
            Event(
                key=f"{table}:{row.get(id_col)}",
                kind=choose_kind(table),
                title=title,
                description=str(row.get(description_col, "") or "").strip(),
                status=normalize_status(row, columns),
                start_at=str(row.get(start_col)) if start_col and row.get(start_col) is not None else None,
                end_at=str(row.get(end_col)) if end_col and row.get(end_col) is not None else None,
                affected_services=fetch_maintenance_monitors(db, row.get(id_col)) if choose_kind(table) == "maintenance" else [],
            )
        )
    return events


def collect_events() -> List[Event]:
    now = datetime.now(timezone.utc)

    with DBClient() as db:
        candidate_tables = [
            t
            for t in db.list_tables()
            if any(token in t.lower() for token in ["maintenance", "incident"])
            and t.lower() not in {"incident_monitor", "maintenance_monitor"}
        ]

        events: List[Event] = []
        for table in candidate_tables:
            events.extend(fetch_events_from_table(db, table, now))

    unique: Dict[str, Event] = {e.key: e for e in events}
    return list(unique.values())


def find_table_name(tables: List[str], include_tokens: Iterable[str], exclude_tokens: Iterable[str] = ()) -> Optional[str]:
    include = [t.lower() for t in include_tokens]
    exclude = [t.lower() for t in exclude_tokens]
    for table in tables:
        lower = table.lower()
        if all(token in lower for token in include) and not any(token in lower for token in exclude):
            return table
    return None


def find_preferred_table_name(
    tables: List[str],
    preferred_names: Iterable[str],
    include_tokens: Iterable[str],
    exclude_tokens: Iterable[str] = (),
) -> Optional[str]:
    lookup = {table.lower(): table for table in tables}
    for name in preferred_names:
        if name.lower() in lookup:
            return lookup[name.lower()]
    return find_table_name(tables, include_tokens, exclude_tokens)


def find_exact_table_name(tables: List[str], name: str) -> Optional[str]:
    lookup = {table.lower(): table for table in tables}
    return lookup.get(name.lower())


def resolve_tag_sync_tables(db: DBClient) -> Optional[Dict[str, Any]]:
    tables = db.list_tables()
    monitor_table = find_exact_table_name(tables, "monitor")
    tag_table = find_preferred_table_name(tables, ["tag"], ["tag"])
    monitor_tag_table = find_preferred_table_name(tables, ["monitor_tag"], ["monitor", "tag"])
    if not monitor_table:
        logger.warning("Skipping status page monitor sync: could not find monitor table.")
        return None
    logger.debug(
        "Using tag tables: tag=%s monitor_tag=%s monitor=%s",
        tag_table,
        monitor_tag_table,
        monitor_table,
    )

    tag_id_col = None
    tag_name_col = None
    link_columns: List[str] = []
    link_tag_id_col = None
    link_monitor_id_col = None
    if tag_table and monitor_tag_table:
        tag_columns = db.table_columns(tag_table)
        tag_id_col = first_existing(tag_columns, ["id", "tag_id"])
        tag_name_col = first_existing(tag_columns, ["name", "value", "tag"])
        if not tag_id_col or not tag_name_col:
            logger.warning("Skipping tag exclusions: tag table missing id/name columns.")
            tag_table = None
            monitor_tag_table = None
        else:
            link_columns = db.table_columns(monitor_tag_table)
            link_tag_id_col = first_existing(link_columns, ["tag_id", "tagId", "tag"])
            link_monitor_id_col = first_existing(link_columns, ["monitor_id", "monitorId", "monitor"])
            if not link_tag_id_col or not link_monitor_id_col:
                logger.warning("Skipping tag exclusions: monitor-tag table missing monitor/tag columns.")
                tag_table = None
                monitor_tag_table = None

    return {
        "tag_table": tag_table,
        "tag_id_col": tag_id_col,
        "tag_name_col": tag_name_col,
        "monitor_tag_table": monitor_tag_table,
        "link_columns": link_columns,
        "link_tag_id_col": link_tag_id_col,
        "link_monitor_id_col": link_monitor_id_col,
        "monitor_table": monitor_table,
    }


def parse_csv_values(raw: str) -> List[str]:
    return [part.strip() for part in raw.split(",") if part.strip()]


def collect_excluded_monitor_ids(db: DBClient, excluded_tag_names: List[str]) -> List[int]:
    tag_context = resolve_tag_sync_tables(db)
    if not tag_context or not excluded_tag_names:
        return []
    if not tag_context["tag_table"] or not tag_context["monitor_tag_table"]:
        logger.warning("Skipping excluded-tag filtering because tag tables are unavailable.")
        return []

    excluded_tag_names_lookup = {tag_name.lower() for tag_name in excluded_tag_names}
    tag_rows = db.fetch_table_rows(tag_context["tag_table"])
    matching_tag_ids = {
        row[tag_context["tag_id_col"]]
        for row in tag_rows
        if str(row.get(tag_context["tag_name_col"], "")).strip().lower() in excluded_tag_names_lookup
    }
    if not matching_tag_ids:
        logger.info(
            "None of the excluded tags %s were found in table '%s'.",
            ", ".join(excluded_tag_names),
            tag_context["tag_table"],
        )
        return []
    logger.debug("Matched excluded tags %s to tag IDs: %s", excluded_tag_names, sorted(matching_tag_ids))

    monitor_ids = []
    for row in db.fetch_table_rows(tag_context["monitor_tag_table"]):
        if (
            row.get(tag_context["link_tag_id_col"]) in matching_tag_ids
            and row.get(tag_context["link_monitor_id_col"]) is not None
        ):
            monitor_ids.append(int(row[tag_context["link_monitor_id_col"]]))
    resolved_monitor_ids = sorted(set(monitor_ids))
    logger.info(
        "Found %s monitor(s) excluded by tags %s.",
        len(resolved_monitor_ids),
        ", ".join(excluded_tag_names),
    )
    logger.debug("Excluded monitor IDs for tags %s: %s", excluded_tag_names, resolved_monitor_ids)
    return resolved_monitor_ids


def collect_managed_monitor_ids(db: DBClient, excluded_tag_names: List[str]) -> List[int]:
    tag_context = resolve_tag_sync_tables(db)
    if not tag_context:
        return []

    monitor_columns = db.table_columns(tag_context["monitor_table"])
    monitor_id_col = first_existing(monitor_columns, ["id", "monitor_id"])
    monitor_paused_col = first_existing(monitor_columns, ["paused", "is_paused"])
    monitor_active_col = first_existing(monitor_columns, ["active", "is_active", "enabled"])
    if not monitor_id_col:
        logger.warning("Skipping status page monitor sync: monitor table missing id column.")
        return []

    monitor_rows = [row for row in db.fetch_table_rows(tag_context["monitor_table"]) if row.get(monitor_id_col) is not None]
    if not monitor_rows:
        logger.info("No monitors found for status page sync.")
        return []

    active_monitor_ids = []
    for row in monitor_rows:
        monitor_id = int(row[monitor_id_col])
        is_paused = False
        if monitor_paused_col:
            is_paused = bool(row.get(monitor_paused_col))
        elif monitor_active_col:
            is_paused = not bool(row.get(monitor_active_col))
        if not is_paused:
            active_monitor_ids.append(monitor_id)

    excluded_monitor_ids = set(collect_excluded_monitor_ids(db, excluded_tag_names))
    managed_monitor_ids = sorted(monitor_id for monitor_id in active_monitor_ids if monitor_id not in excluded_monitor_ids)
    logger.info(
        "Managing %s active monitor(s) for status page sync; excluded %s by tag.",
        len(managed_monitor_ids),
        len(excluded_monitor_ids),
    )
    return managed_monitor_ids


def sort_monitor_ids_alphabetically(db: DBClient, monitor_ids: List[int]) -> List[int]:
    if not monitor_ids:
        return []

    monitor_table = find_exact_table_name(db.list_tables(), "monitor")
    if not monitor_table:
        logger.warning("Skipping alphabetical status page ordering: could not find monitor table.")
        return monitor_ids

    monitor_columns = db.table_columns(monitor_table)
    monitor_id_col = first_existing(monitor_columns, ["id", "monitor_id"])
    monitor_name_col = first_existing(monitor_columns, ["name", "friendly_name"])
    if not monitor_id_col or not monitor_name_col:
        logger.warning("Skipping alphabetical status page ordering: monitor table missing id/name columns.")
        return monitor_ids

    monitor_lookup = {
        int(row[monitor_id_col]): str(row.get(monitor_name_col, "")).strip()
        for row in db.fetch_table_rows(monitor_table)
        if row.get(monitor_id_col) is not None
    }
    return sorted(monitor_ids, key=lambda monitor_id: (monitor_lookup.get(monitor_id, "").lower(), monitor_id))


def find_default_status_page(db: DBClient, slug: str) -> Optional[Dict[str, Any]]:
    tables = db.list_tables()
    status_page_table = find_exact_table_name(tables, "status_page")
    if not status_page_table:
        return None

    rows = db.fetch_table_rows(status_page_table)
    if not rows:
        return None

    columns = db.table_columns(status_page_table)
    slug_col = first_existing(columns, ["slug", "path"])
    title_col = first_existing(columns, ["title", "name"])
    id_col = first_existing(columns, ["id", "status_page_id"])
    if not id_col:
        return None

    for row in rows:
        if slug_col and str(row.get(slug_col, "")).strip().lower() == slug.lower():
            return {"table": status_page_table, "id_col": id_col, "row": row}
    for row in rows:
        if title_col and str(row.get(title_col, "")).strip().lower() == slug.lower():
            return {"table": status_page_table, "id_col": id_col, "row": row}
    if len(rows) == 1:
        return {"table": status_page_table, "id_col": id_col, "row": rows[0]}
    return None


def sync_managed_monitors_to_default_status_page() -> None:
    manage_status_page = env("MANAGE_STATUS_PAGE", "false").strip().lower() == "true"
    if not manage_status_page:
        return

    excluded_tag_names = parse_csv_values(os.getenv("IGNORE_TAGS", ""))
    default_page_slug = env("STATUS_PAGE_SLUG", "default")
    group_name = env("STATUS_PAGE_GROUP_NAME", "Services")
    sort_alphabetically = env("STATUS_PAGE_SORT_ALPHABETIC", "false").strip().lower() == "true"

    with DBClient() as db:
        managed_monitor_ids = collect_managed_monitor_ids(db, excluded_tag_names)
        if sort_alphabetically:
            managed_monitor_ids = sort_monitor_ids_alphabetically(db, managed_monitor_ids)
        if not managed_monitor_ids:
            logger.info("No active monitors remain after exclusions; stale status page links will be removed.")

        status_page = find_default_status_page(db, default_page_slug)
        if not status_page:
            logger.warning("Skipping status page monitor sync: no status page found for '%s'.", default_page_slug)
            return

        status_page_id = status_page["row"][status_page["id_col"]]
        tables = db.list_tables()
        group_table = find_exact_table_name(tables, "group")
        monitor_link_table = find_exact_table_name(tables, "monitor_group")
        if not group_table or not monitor_link_table:
            logger.warning(
                "Skipping status page monitor sync: required tables not found. "
                "group_table=%s monitor_link_table=%s available_tables=%s",
                group_table,
                monitor_link_table,
                ", ".join(sorted(tables)),
            )
            return
        logger.debug(
            "Using status page tables: status_page=%s group=%s monitor_link=%s",
            status_page["table"],
            group_table,
            monitor_link_table,
        )

        group_columns = db.table_columns(group_table)
        group_id_col = first_existing(group_columns, ["id", "status_page_group_id"])
        group_status_page_col = first_existing(group_columns, ["status_page_id", "statusPageId"])
        group_name_col = first_existing(group_columns, ["name", "title"])
        group_weight_col = first_existing(group_columns, ["weight", "order"])
        group_public_col = first_existing(group_columns, ["public", "is_public"])
        if not group_id_col or not group_status_page_col:
            logger.warning("Skipping status page monitor sync: status page group table missing required columns.")
            return

        groups = [g for g in db.fetch_table_rows(group_table) if g.get(group_status_page_col) == status_page_id]
        target_group = None
        if group_name_col:
            for group in groups:
                if str(group.get(group_name_col, "")).strip().lower() == group_name.lower():
                    target_group = group
                    break
        if not target_group and groups:
            target_group = groups[0]

        if not target_group:
            insert_columns = [group_status_page_col]
            insert_values: List[Any] = [status_page_id]
            if group_name_col:
                insert_columns.append(group_name_col)
                insert_values.append(group_name)
            if group_weight_col:
                next_weight = max((int(g.get(group_weight_col, 0) or 0) for g in groups), default=0) + 1
                insert_columns.append(group_weight_col)
                insert_values.append(next_weight)
            if group_public_col:
                insert_columns.append(group_public_col)
                insert_values.append(1)

            placeholders = ", ".join(["%s"] * len(insert_columns))
            sql = f"INSERT INTO `{group_table}` ({', '.join(f'`{c}`' for c in insert_columns)}) VALUES ({placeholders})"
            with db.conn.cursor() as cur:
                cur.execute(sql, insert_values)
                group_id = cur.lastrowid
            target_group = {group_id_col: group_id}
            logger.info("Created status page group '%s' for page %s.", group_name, status_page_id)

        target_group_id = target_group[group_id_col]

        link_columns = db.table_columns(monitor_link_table)
        link_group_col = first_existing(link_columns, ["group_id", "status_page_group_id"])
        link_monitor_col = first_existing(link_columns, ["monitor_id", "monitorId", "monitor"])
        link_weight_col = first_existing(link_columns, ["weight", "order"])
        if not link_monitor_col or not link_group_col:
            logger.warning("Skipping status page monitor sync: status page monitor table missing required columns.")
            return

        existing_links = db.fetch_table_rows(monitor_link_table)
        link_id_col = first_existing(link_columns, ["id", "status_page_monitor_id"])
        existing_monitor_ids = set()
        managed_links: List[Dict[str, Any]] = []
        managed_links_by_monitor_id: Dict[int, Dict[str, Any]] = {}
        max_weight = 0
        for link in existing_links:
            in_group = link.get(link_group_col) == target_group_id
            if in_group:
                if link.get(link_monitor_col) is not None:
                    monitor_id = int(link[link_monitor_col])
                    existing_monitor_ids.add(monitor_id)
                    managed_links_by_monitor_id[monitor_id] = link
                if link_weight_col:
                    max_weight = max(max_weight, int(link.get(link_weight_col, 0) or 0))
            if in_group:
                managed_links.append(link)

        managed_monitor_id_set = set(managed_monitor_ids)
        missing = [mid for mid in managed_monitor_ids if mid not in existing_monitor_ids]
        reordered = 0
        if sort_alphabetically and link_weight_col:
            for weight, monitor_id in enumerate(managed_monitor_ids, start=1):
                existing_link = managed_links_by_monitor_id.get(monitor_id)
                if existing_link:
                    current_weight = int(existing_link.get(link_weight_col, 0) or 0)
                    if current_weight != weight:
                        if link_id_col and existing_link.get(link_id_col) is not None:
                            sql = f"UPDATE `{monitor_link_table}` SET `{link_weight_col}` = %s WHERE `{link_id_col}` = %s"
                            values = [weight, existing_link[link_id_col]]
                        else:
                            sql = (
                                f"UPDATE `{monitor_link_table}` SET `{link_weight_col}` = %s "
                                f"WHERE `{link_monitor_col}` = %s AND `{link_group_col}` = %s"
                            )
                            values = [weight, monitor_id, target_group_id]
                        with db.conn.cursor() as cur:
                            cur.execute(sql, values)
                        reordered += 1
                    continue

                insert_columns = [link_monitor_col, link_group_col, link_weight_col]
                insert_values: List[Any] = [monitor_id, target_group_id, weight]
                placeholders = ", ".join(["%s"] * len(insert_columns))
                sql = (
                    f"INSERT INTO `{monitor_link_table}` "
                    f"({', '.join(f'`{c}`' for c in insert_columns)}) VALUES ({placeholders})"
                )
                with db.conn.cursor() as cur:
                    cur.execute(sql, insert_values)
        else:
            for monitor_id in missing:
                insert_columns = [link_monitor_col]
                insert_values: List[Any] = [monitor_id]
                insert_columns.append(link_group_col)
                insert_values.append(target_group_id)
                if link_weight_col:
                    max_weight += 1
                    insert_columns.append(link_weight_col)
                    insert_values.append(max_weight)

                placeholders = ", ".join(["%s"] * len(insert_columns))
                sql = f"INSERT INTO `{monitor_link_table}` ({', '.join(f'`{c}`' for c in insert_columns)}) VALUES ({placeholders})"
                with db.conn.cursor() as cur:
                    cur.execute(sql, insert_values)

        stale_links = [
            link
            for link in managed_links
            if link.get(link_monitor_col) is not None and int(link[link_monitor_col]) not in managed_monitor_id_set
        ]
        for link in stale_links:
            delete_clauses: List[str] = []
            delete_values: List[Any] = []
            if link_id_col and link.get(link_id_col) is not None:
                delete_clauses.append(f"`{link_id_col}` = %s")
                delete_values.append(link[link_id_col])
            else:
                delete_clauses.append(f"`{link_monitor_col}` = %s")
                delete_values.append(link[link_monitor_col])
                delete_clauses.append(f"`{link_group_col}` = %s")
                delete_values.append(target_group_id)

            sql = f"DELETE FROM `{monitor_link_table}` WHERE {' AND '.join(delete_clauses)}"
            with db.conn.cursor() as cur:
                cur.execute(sql, delete_values)
        if missing or stale_links or reordered:
            logger.info(
                "Synced status page '%s': added %s monitor(s), removed %s monitor(s), reordered %s monitor(s).",
                default_page_slug,
                len(missing),
                len(stale_links),
                reordered,
            )
        else:
            logger.info(
                "Status page '%s' already matches active monitors excluding tags %s.",
                default_page_slug,
                ", ".join(excluded_tag_names),
            )


def gh_request(method: str, url: str, token: str, **kwargs):
    headers = kwargs.pop("headers", {})
    headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )
    resp = requests.request(method, url, headers=headers, timeout=30, **kwargs)
    if resp.status_code >= 400:
        raise RuntimeError(f"GitHub API error {resp.status_code}: {resp.text[:500]}")
    return resp


def event_fingerprint(event: Event) -> str:
    payload = json.dumps(
        {
            "title": event.title,
            "description": event.description,
            "status": event.status,
            "start_at": event.start_at,
            "end_at": event.end_at,
            "affected_services": event.affected_services,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def has_known_value(value: Optional[str]) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() not in {"", "unknown"}


def affected_services_title() -> str:
    return f"Affected {env('STATUS_PAGE_GROUP_NAME', 'Services')}"


def event_lines(event: Event, include_services: bool = True) -> List[str]:
    lines = [
        f"- **Type**: {event.kind}",
        f"- **Status**: {event.status}",
    ]
    if has_known_value(event.start_at):
        lines.append(f"- **Starts**: {event.start_at}")
    if has_known_value(event.end_at):
        lines.append(f"- **Ends**: {event.end_at}")
    lines.extend(["", "### Details", event.description or "(No description provided)"])
    if include_services and event.affected_services:
        lines.extend(["", f"### {affected_services_title()}", ", ".join(event.affected_services)])
    return lines


def issue_body(event: Event) -> str:
    lines = event_lines(event)
    lines.extend(
        [
            "",
            f"{MARKER_PREFIX}{event.key} -->",
            f"<!-- kuma-fingerprint:{event_fingerprint(event)} -->",
        ]
    )
    return "\n".join(lines)


def issue_update_comment(event: Event, include_services: bool) -> str:
    lines = event_lines(event, include_services=include_services)
    lines.extend(["", f"<!-- kuma-fingerprint:{event_fingerprint(event)} -->"])
    return "\n".join(lines)


def parse_event_key(body: str) -> Optional[str]:
    match = re.search(r"<!-- kuma-event-key:(.*?) -->", body or "")
    return match.group(1).strip() if match else None


def parse_fingerprint(body: str) -> Optional[str]:
    match = re.search(r"<!-- kuma-fingerprint:(.*?) -->", body or "")
    return match.group(1).strip() if match else None


def parse_affected_services(body: str) -> List[str]:
    title = re.escape(affected_services_title())
    match = re.search(rf"^### {title}\s*$([\s\S]*?)(?:^### |\Z)", body or "", flags=re.MULTILINE)
    if not match:
        return []
    services: List[str] = []
    for line in match.group(1).splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            services.append(stripped[2:].strip())
    return services


def latest_managed_state(repo: str, token: str, issue: dict) -> Dict[str, Optional[str]]:
    url = issue.get("comments_url")
    latest_body = issue.get("body", "") or ""
    latest_fingerprint = parse_fingerprint(latest_body)

    while url:
        resp = gh_request("GET", url, token, params={"per_page": 100})
        comments = resp.json()
        for comment in comments:
            fingerprint = parse_fingerprint(comment.get("body", ""))
            if fingerprint:
                latest_fingerprint = fingerprint
                latest_body = comment.get("body", "") or ""
        url = resp.links.get("next", {}).get("url")

    return {"fingerprint": latest_fingerprint, "body": latest_body}


def list_managed_open_issues(repo: str, token: str, label: str) -> Dict[str, dict]:
    url = f"https://api.github.com/repos/{repo}/issues"
    params = {"state": "open", "labels": label, "per_page": 100}
    issues_by_key: Dict[str, dict] = {}

    while True:
        resp = gh_request("GET", url, token, params=params)
        for issue in resp.json():
            if "pull_request" in issue:
                continue
            key = parse_event_key(issue.get("body", ""))
            if key:
                issues_by_key[key] = issue

        next_link = resp.links.get("next", {}).get("url")
        if not next_link:
            break
        url = next_link
        params = None

    return issues_by_key


def ensure_issue(repo: str, token: str, label: str, event: Event, existing: Optional[dict]) -> None:
    desired_body = issue_body(event)
    desired_title = f"[{event.kind}] {event.title}"
    desired_fingerprint = event_fingerprint(event)

    if existing:
        current_state = latest_managed_state(repo, token, existing)
        current_fingerprint = current_state["fingerprint"]
        if current_fingerprint == desired_fingerprint and existing.get("title") == desired_title:
            return
        if existing.get("title") != desired_title:
            gh_request(
                "PATCH",
                f"https://api.github.com/repos/{repo}/issues/{existing['number']}",
                token,
                json={"title": desired_title},
            )
        if current_fingerprint != desired_fingerprint:
            include_services = parse_affected_services(current_state["body"] or "") != event.affected_services
            gh_request(
                "POST",
                f"https://api.github.com/repos/{repo}/issues/{existing['number']}/comments",
                token,
                json={"body": issue_update_comment(event, include_services=include_services)},
            )
        logger.info("Updated issue #%s for %s.", existing["number"], event.key)
        return

    resp = gh_request(
        "POST",
        f"https://api.github.com/repos/{repo}/issues",
        token,
        json={"title": desired_title, "body": desired_body, "labels": [label]},
    )
    logger.info("Created issue #%s for %s.", resp.json()["number"], event.key)


def close_issue(repo: str, token: str, issue_number: int) -> None:
    gh_request(
        "POST",
        f"https://api.github.com/repos/{repo}/issues/{issue_number}/comments",
        token,
        json={"body": "Auto-closing because this incident/maintenance is no longer active."},
    )
    gh_request(
        "PATCH",
        f"https://api.github.com/repos/{repo}/issues/{issue_number}",
        token,
        json={"state": "closed"},
    )
    logger.info("Closed issue #%s.", issue_number)


def sync_once() -> None:
    gh_repo = env("GITHUB_REPO", required=True)
    gh_token = env("GITHUB_TOKEN", required=True)
    label = env("GITHUB_LABEL", MANAGED_LABEL)
    auto_close = env("AUTO_CLOSE_RESOLVED", "true").lower() == "true"

    logger.info("Starting sync for repository '%s'.", gh_repo)
    events = collect_events()
    events_by_key = {e.key: e for e in events}
    logger.info("Collected %s active event(s) from Uptime Kuma.", len(events_by_key))
    managed_open = list_managed_open_issues(gh_repo, gh_token, label)
    logger.info("Loaded %s managed open issue(s) with label '%s'.", len(managed_open), label)

    for key, event in events_by_key.items():
        ensure_issue(gh_repo, gh_token, label, event, managed_open.get(key))

    if auto_close:
        for key in sorted(set(managed_open.keys()) - set(events_by_key.keys())):
            close_issue(gh_repo, gh_token, managed_open[key]["number"])
    sync_managed_monitors_to_default_status_page()
    logger.info("Sync complete.")


def main() -> None:
    logging.basicConfig(
        level=getattr(logging, env("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    interval = int(env("POLL_INTERVAL_SECONDS", "120"))
    run_once = env("RUN_ONCE", "false").lower() == "true"

    if run_once:
        sync_once()
        return

    while True:
        try:
            sync_once()
        except Exception as exc:
            logger.exception("Sync failed: %s", exc)
        time.sleep(interval)


if __name__ == "__main__":
    main()
