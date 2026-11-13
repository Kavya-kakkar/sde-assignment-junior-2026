
from __future__ import annotations

import csv
import json
import os
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from collections import defaultdict

from interaction_ledger.config import load_assignment_configuration
from interaction_ledger.database import initialize_database
from interaction_ledger.crypto import PiiProtector
from interaction_ledger.leads import normalize_phone

ROOT = Path("..").resolve()
DATA = ROOT / "agentic-au-data"
OUTPUT = Path("output")
DB_PATH = Path("interaction_ledger.db")
SECRETS = Path(".local-secrets.json")
IST = ZoneInfo("Asia/Kolkata")


def load_protector():
    keys = json.loads(SECRETS.read_text(encoding="utf-8"))
    os.environ.update(keys)
    return PiiProtector.from_environment()


def utc_timestamp(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def local_datetime(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def write_csv(filename, headers, rows):
    OUTPUT.mkdir(exist_ok=True)
    with (OUTPUT / filename).open(
        "w", encoding="utf-8-sig", newline=""
    ) as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def load_catalogue(connection, config):
    for (workflow, code), item in config.dispositions.items():
        connection.execute(
            """
            INSERT OR REPLACE INTO disposition_catalogue
            (workflow_id, code, label, is_terminal, is_success)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                workflow,
                code,
                item.label,
                int(item.is_terminal),
                int(item.is_success),
            ),
        )


def find_lead(connection, workflow, phone_hmac):
    if not phone_hmac:
        return None
    row = connection.execute(
        """
        SELECT id FROM leads
        WHERE workflow_id = ? AND phone_hmac = ?
        """,
        (workflow, phone_hmac),
    ).fetchone()
    return row["id"] if row else None


def ingest_event_file(database, path, protector, known_codes):
    content = path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    source = str(path.resolve())

    with database.transaction() as con:
        con.execute(
            """
            INSERT OR IGNORE INTO ingestion_files
            (source_path, content_sha256, file_kind, workflow_id)
            VALUES (?, ?, 'events', NULL)
            """,
            (source, digest),
        )

        file_row = con.execute(
            """
            SELECT id, processing_state FROM ingestion_files
            WHERE source_path = ? AND content_sha256 = ?
            """,
            (source, digest),
        ).fetchone()

        file_id = file_row["id"]

        if file_row["processing_state"] == "completed":
            print(f"Already processed: {path.name}")
            return

        counts = defaultdict(int)

        with path.open("r", encoding="utf-8") as source_file:
            for line_number, line in enumerate(source_file, 1):
                try:
                    event = json.loads(line)
                    if not isinstance(event, dict) or not event.get("event_id"):
                        raise ValueError("missing_event_id")
                except (json.JSONDecodeError, ValueError):
                    con.execute(
                        """
                        INSERT INTO event_lines
                        (ingestion_file_id, line_number, event_id,
                         processing_status, detail)
                        VALUES (?, ?, NULL, 'malformed', ?)
                        """,
                        (file_id, line_number, "invalid_json_or_missing_event_id"),
                    )
                    counts["malformed"] += 1
                    continue

                event_id = str(event["event_id"])

                existing = con.execute(
                    "SELECT event_id FROM events WHERE event_id = ?",
                    (event_id,),
                ).fetchone()

                if existing:
                    con.execute(
                        """
                        INSERT INTO event_lines
                        (ingestion_file_id, line_number, event_id,
                         processing_status, detail)
                        VALUES (?, ?, ?, 'duplicate', ?)
                        """,
                        (file_id, line_number, event_id, "duplicate_event_id"),
                    )
                    counts["duplicate"] += 1
                    continue

                workflow = event["workflow_id"]
                event_type = event["event_type"]
                call_id = event.get("call_id")
                payload = event.get("payload") or {}
                code = payload.get("disposition_code")

                unknown = (
                    event_type in ("disposition.set", "disposition.override")
                    and (workflow, code) not in known_codes
                )

                phone_hmac = None
                phone_ciphertext = None
                lead_id = None

                phone = event.get("phone")
                if phone:
                    try:
                        canonical = normalize_phone(phone)
                        phone_hmac = protector.phone_hmac(canonical)
                        phone_ciphertext = protector.encrypt_text(canonical)
                        lead_id = find_lead(con, workflow, phone_hmac)
                    except ValueError:
                        pass

                occurred_at = utc_timestamp(event["occurred_at"])
                status = (
                    "unknown_disposition" if unknown
                    else "orphan" if lead_id is None
                    else "accepted"
                )

                con.execute(
                    """
                    INSERT INTO events (
                        event_id, workflow_id, lead_id, call_id,
                        event_type, occurred_at, phone_hmac,
                        phone_ciphertext, payload_json, is_reportable,
                        ingestion_file_id, source_line_number
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_id, workflow, lead_id, call_id,
                        event_type, occurred_at, phone_hmac,
                        phone_ciphertext,
                        json.dumps(payload, ensure_ascii=False),
                        int(not unknown), file_id, line_number,
                    ),
                )

                detail = f"unknown_disposition:{code}" if unknown else None

                con.execute(
                    """
                    INSERT INTO event_lines
                    (ingestion_file_id, line_number, event_id,
                     processing_status, detail)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (file_id, line_number, event_id, status, detail),
                )
                counts[status] += 1

        con.execute(
            """
            UPDATE ingestion_files
            SET processing_state = 'completed',
                completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (file_id,),
        )

    print(f"{path.name}: {dict(counts)}")


def resolve_orphans(database):
    with database.transaction() as con:
        rows = con.execute(
            """
            SELECT event_id, workflow_id, phone_hmac, call_id
            FROM events
            WHERE lead_id IS NULL
            """
        ).fetchall()

        resolved = 0

        for event in rows:
            lead_id = find_lead(
                con, event["workflow_id"], event["phone_hmac"]
            )

            if lead_id is None and event["call_id"]:
                linked = con.execute(
                    """
                    SELECT lead_id FROM events
                    WHERE workflow_id = ? AND call_id = ?
                      AND lead_id IS NOT NULL
                    LIMIT 1
                    """,
                    (event["workflow_id"], event["call_id"]),
                ).fetchone()

                if linked:
                    lead_id = linked["lead_id"]

            if lead_id is None:
                continue

            con.execute(
                "UPDATE events SET lead_id = ? WHERE event_id = ?",
                (lead_id, event["event_id"]),
            )

            con.execute(
                """
                UPDATE event_lines
                SET processing_status = 'accepted',
                    detail = 'resolved_to_lead'
                WHERE event_id = ?
                  AND processing_status = 'orphan'
                """,
                (event["event_id"],),
            )
            resolved += 1

        print("Resolved orphan events:", resolved)


def load_events(connection):
    rows = connection.execute(
        """
        SELECT e.*, c.is_terminal, c.is_success
        FROM events e
        LEFT JOIN disposition_catalogue c
          ON c.workflow_id = e.workflow_id
         AND c.code = json_extract(e.payload_json, '$.disposition_code')
        WHERE e.lead_id IS NOT NULL AND e.is_reportable = 1
        ORDER BY e.occurred_at, e.event_id
        """
    ).fetchall()

    by_lead = defaultdict(list)
    for row in rows:
        by_lead[(row["workflow_id"], row["lead_id"])].append(dict(row))
    return by_lead


def generate_reports(database, config, protector):
    OUTPUT.mkdir(exist_ok=True)

    with database.connect() as con:
        by_lead = load_events(con)

        leads = con.execute(
            """
            SELECT l.*, b.batch_id
            FROM leads l
            JOIN batches b
              ON b.workflow_id = l.workflow_id
             AND b.batch_id = l.batch_id
            ORDER BY l.workflow_id, l.id
            """
        ).fetchall()

        lead_report = []
        batch_counts = defaultdict(lambda: {
            "total_leads": 0,
            "yet_to_reach_out": 0,
            "in_progress_telephony_failed": 0,
            "in_progress_dialed": 0,
            "completed": 0,
        })

        daily = defaultdict(lambda: {
            "total_calls": 0,
            "connected_calls": 0,
            "success_leads": set(),
        })

        for lead in leads:
            workflow = lead["workflow_id"]
            lead_id = lead["id"]
            key = (workflow, lead_id)
            events = by_lead.get(key, [])

            schema = config.workflows[workflow]
            attrs = json.loads(lead["non_pii_attributes_json"])
            pii = json.loads(
                protector.decrypt_text(lead["pii_ciphertext"])
            )

            calls = defaultdict(list)
            dispositions = []

            for event in events:
                if event["event_type"].startswith("call."):
                    if event["call_id"]:
                        calls[event["call_id"]].append(event)

                if event["event_type"] in (
                    "disposition.set", "disposition.override"
                ):
                    dispositions.append(event)

            initiated = {
                call_id: next(
                    (e for e in call_events
                     if e["event_type"] == "call.initiated"),
                    None,
                )
                for call_id, call_events in calls.items()
            }
            initiated = {
                call_id: event
                for call_id, event in initiated.items()
                if event is not None
            }

            connected_ids = {
                call_id for call_id, call_events in calls.items()
                if any(e["event_type"] == "call.connected"
                       for e in call_events)
            }

            failed_ids = {
                call_id for call_id, call_events in calls.items()
                if any(e["event_type"] == "call.telephony_failed"
                       for e in call_events)
            }

            dispositions.sort(key=lambda e: (e["occurred_at"], e["event_id"]))
            overrides = [
                e for e in dispositions
                if e["event_type"] == "disposition.override"
            ]
            sets = [
                e for e in dispositions
                if e["event_type"] == "disposition.set"
            ]

            effective = overrides[-1] if overrides else (
                sets[-1] if sets else None
            )

            current = (
                json.loads(effective["payload_json"]).get("disposition_code")
                if effective else ""
            )

            ever_success = any(
                e["is_success"] == 1 for e in dispositions
            )

            call_count = len(initiated)
            connected_count = len(
                connected_ids.intersection(initiated.keys())
            )

            if call_count == 0:
                bucket = "yet_to_reach_out"
            elif effective and effective["is_terminal"]:
                bucket = "completed"
            elif len(failed_ids.intersection(initiated.keys())) == call_count:
                bucket = "in_progress_telephony_failed"
            else:
                bucket = "in_progress_dialed"

            batch_key = (workflow, lead["batch_id"])
            batch_counts[batch_key]["total_leads"] += 1
            batch_counts[batch_key][bucket] += 1

            phone = pii.get(schema.phone_field.name) or ""
            masked = "******" + phone[-4:] if phone else ""

            reference = attrs.get(schema.reference_field.name) or ""

            initiated_events = list(initiated.values())
            last_call = max(
                initiated_events,
                key=lambda e: e["occurred_at"],
                default=None,
            )

            last_call_at = ""
            if last_call:
                last_call_at = local_datetime(
                    last_call["occurred_at"]
                ).isoformat()

            lead_report.append({
                "workflow_id": workflow,
                "external_ref": reference,
                "batch_id": lead["batch_id"],
                "phone_masked": masked,
                "bucket": bucket,
                "total_calls": call_count,
                "connected_calls": connected_count,
                "last_call_at": last_call_at,
                "current_disposition": current,
                "ever_success": str(bool(ever_success)).lower(),
            })

            for call_id, event in initiated.items():
                date = local_datetime(event["occurred_at"]).date().isoformat()
                daily[(workflow, date)]["total_calls"] += 1
                if call_id in connected_ids:
                    daily[(workflow, date)]["connected_calls"] += 1

            # Count each successful lead on the date of its latest
            # successful disposition event.
            successful = [
                e for e in dispositions if e["is_success"] == 1
            ]
            if successful:
                success_event = successful[-1]
                date = local_datetime(
                    success_event["occurred_at"]
                ).date().isoformat()
                daily[(workflow, date)]["success_leads"].add(lead_id)

        batch_rows = []
        for (workflow, batch_id), counts in sorted(batch_counts.items()):
            batch_rows.append({
                "workflow_id": workflow,
                "batch_id": batch_id,
                **counts,
            })

        daily_rows = []
        for (workflow, date), values in sorted(daily.items()):
            total = values["total_calls"]
            connected = values["connected_calls"]
            daily_rows.append({
                "workflow_id": workflow,
                "date": date,
                "total_calls": total,
                "connected_calls": connected,
                "connectivity_rate": f"{connected / total:.4f}" if total else "0.0000",
                "success_leads": len(values["success_leads"]),
            })

        lead_report.sort(
            key=lambda r: (r["workflow_id"], r["external_ref"])
        )

        write_csv(
            "batch_summary.csv",
            [
                "workflow_id", "batch_id", "total_leads",
                "yet_to_reach_out", "in_progress_telephony_failed",
                "in_progress_dialed", "completed",
            ],
            batch_rows,
        )

        write_csv(
            "daily_overview.csv",
            [
                "workflow_id", "date", "total_calls",
                "connected_calls", "connectivity_rate", "success_leads",
            ],
            daily_rows,
        )

        write_csv(
            "lead_report.csv",
            [
                "workflow_id", "external_ref", "batch_id",
                "phone_masked", "bucket", "total_calls",
                "connected_calls", "last_call_at",
                "current_disposition", "ever_success",
            ],
            lead_report,
        )

        rejections = con.execute(
            """
            SELECT workflow_id, batch_id, row_number, reason
            FROM rejections
            ORDER BY workflow_id, batch_id, row_number
            """
        ).fetchall()

        write_csv(
            "rejections.csv",
            ["workflow_id", "batch_id", "row_number", "reason"],
            [dict(r) for r in rejections],
        )

        quality = {
            "total_lead_rows": con.execute(
                "SELECT COUNT(*) FROM lead_lines"
            ).fetchone()[0],
            "rejected_lead_rows": con.execute(
                "SELECT COUNT(*) FROM lead_lines WHERE outcome='rejected'"
            ).fetchone()[0],
            "rejection_reasons": {
                r["reason"]: r["count"]
                for r in con.execute(
                    """
                    SELECT reason, COUNT(*) AS count
                    FROM rejections GROUP BY reason ORDER BY reason
                    """
                )
            },
            "duplicate_leads_merged": con.execute(
                """
                SELECT COUNT(*) FROM lead_lines
                WHERE outcome='merged_duplicate'
                """
            ).fetchone()[0],
            "field_warnings": con.execute(
                "SELECT COALESCE(SUM(field_warning_count), 0) FROM lead_lines"
            ).fetchone()[0],
            "total_event_lines": con.execute(
                "SELECT COUNT(*) FROM event_lines"
            ).fetchone()[0],
            "malformed_events": con.execute(
                "SELECT COUNT(*) FROM event_lines WHERE processing_status='malformed'"
            ).fetchone()[0],
            "duplicate_events": con.execute(
                "SELECT COUNT(*) FROM event_lines WHERE processing_status='duplicate'"
            ).fetchone()[0],
            "orphan_events": con.execute(
                "SELECT COUNT(*) FROM event_lines WHERE processing_status='orphan'"
            ).fetchone()[0],
            "unknown_disposition_events": con.execute(
                """
                SELECT COUNT(*) FROM event_lines
                WHERE processing_status='unknown_disposition'
                """
            ).fetchone()[0],
        }

        (OUTPUT / "data_quality.json").write_text(
            json.dumps(quality, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        print("\nReport generation complete.")
        print("Leads:", len(lead_report))
        print("Batches:", len(batch_rows))
        print("Daily rows:", len(daily_rows))
        print("Data quality:", json.dumps(quality, indent=2))


def main():
    config = load_assignment_configuration(ROOT)
    protector = load_protector()
    database = initialize_database(DB_PATH)

    known_codes = set(config.dispositions.keys())

    with database.transaction() as con:
        load_catalogue(con, config)

    for drop in ("drop_1", "drop_2"):
        folder = DATA / drop / "events"
        for path in sorted(folder.glob("*.jsonl")):
            ingest_event_file(
                database, path, protector, known_codes
            )

    resolve_orphans(database)
    generate_reports(database, config, protector)

    print("\nOUTPUT FILES:")
    for path in sorted(OUTPUT.iterdir()):
        print(path.resolve())


if __name__ == "__main__":
    main()
