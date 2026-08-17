from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import time
from datetime import date, datetime
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import psycopg
import requests
import streamlit as st
from psycopg.rows import dict_row

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("thanos")

STREAM_URL = os.getenv("STREAM_URL", "https://stream.companieshouse.gov.uk/companies")
REST_BASE_URL = os.getenv("REST_BASE_URL", "https://api.company-information.service.gov.uk").rstrip("/")
STREAM_KEY = os.getenv("COMPANIES_HOUSE_STREAM_API_KEY")
REST_KEY = os.getenv("COMPANIES_HOUSE_REST_API_KEY")
STREAM_ENABLED = os.getenv("STREAM_ENABLED", "true").lower() == "true"
ENRICHMENT_ENABLED = os.getenv("ENRICHMENT_ENABLED", "true").lower() == "true"

TARGET_SIC_CODES = {
    "62012", "63110", "64209", "64301", "64999", "72110",
}

RESTRICTED_SIC_CODES = {
    "46110", "46120", "46130", "46140", "46150", "46160", "46170", "46180", "46190",
    "46210", "46220", "46230", "46240", "46310", "46320", "46330", "46341", "46342",
    "46350", "46360", "46370", "46380", "46390", "46410", "46420", "46431", "46439",
    "46440", "46450", "46460", "46470", "46480", "46499", "46510", "46520", "46530",
    "46610", "46620", "46630", "46640", "46650", "46660", "46690", "46711", "46719",
    "46720", "46730", "46740", "46750", "46900", "10110", "10130", "10310", "10410",
    "10511", "10512", "10611", "10612", "10840", "10850", "10890", "10920", "13100",
    "13200", "13300", "13921", "13923", "13960", "14131", "15110", "16290", "19200",
    "20110", "20120", "20130", "20140", "20150", "20160", "20170", "20200", "20301",
    "20302", "20411", "20412", "20590", "21100", "22210", "22290", "23190", "23910",
    "23990", "24100", "24200", "24310", "24320", "24330", "24340", "24410", "24420",
    "24430", "24440", "24450", "24460", "24510", "25110", "25210", "25500", "25990",
    "26110", "26200", "26300", "26511", "26512", "26600", "27110", "27200", "28110",
    "28290", "28300", "28990", "29100", "29310", "30110", "30300", "31090", "32990",
}

BUZZWORDS = (
    "ai", "capital", "europe", "global", "group", "holdings", "inc", "labs",
    "london", "pty", "pvt", "technologies", "technology", "uk",
)

TARGET_COUNTRIES = ("eu", "eea", "usa", "india")
WORKER_NAME = "company_stream_worker"
_worker_lock = threading.Lock()
_worker_thread: threading.Thread | None = None
_stop_event = threading.Event()


def db_connection() -> psycopg.Connection:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg.connect(url, row_factory=dict_row, connect_timeout=15)


def db_fetch_one(sql: str, params: Iterable[Any] | None = None) -> dict[str, Any] | None:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


def db_fetch_all(sql: str, params: Iterable[Any] | None = None) -> list[dict[str, Any]]:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())


def db_write(sql: str, params: Iterable[Any] | None = None) -> None:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()


def normalize(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def normalized_sic(value: Any) -> str:
    return re.sub(r"\D", "", str(value or ""))


def token_matches(text: str, words: Iterable[str]) -> list[str]:
    normalized = normalize(text)
    return sorted({
        normalize(word)
        for word in words
        if normalize(word)
        and re.search(rf"(?<![a-z0-9]){re.escape(normalize(word))}(?![a-z0-9])", normalized)
    })


def matching_sics(codes: Iterable[Any], configured: Iterable[Any]) -> list[str]:
    wanted = {normalized_sic(code) for code in configured if normalized_sic(code)}
    return sorted({normalized_sic(code) for code in codes if normalized_sic(code) in wanted})


def uk_today() -> date:
    return datetime.now(ZoneInfo("Europe/London")).date()


def parse_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def update_status(status: str, error: str | None = None) -> None:
    now = datetime.utcnow()
    db_write(
        """
        insert into public.worker_status
            (worker_name, process_id, status, heartbeat_at, last_error, updated_at)
        values (%s, %s, %s, %s, %s, %s)
        on conflict (worker_name) do update set
            process_id = excluded.process_id,
            status = excluded.status,
            heartbeat_at = excluded.heartbeat_at,
            last_error = excluded.last_error,
            updated_at = excluded.updated_at
        """,
        (WORKER_NAME, str(os.getpid()), status, now, error, now),
    )


def get_checkpoint() -> int | None:
    row = db_fetch_one("select timepoint from public.stream_checkpoints where stream_name = 'companies'")
    return int(row["timepoint"]) if row and row.get("timepoint") is not None else None


def save_checkpoint(timepoint: int) -> None:
    now = datetime.utcnow()
    db_write(
        """
        insert into public.stream_checkpoints
            (stream_name, timepoint, connection_status, last_event_at, last_heartbeat_at, updated_at)
        values ('companies', %s, 'connected', %s, %s, %s)
        on conflict (stream_name) do update set
            timepoint = excluded.timepoint,
            connection_status = 'connected',
            last_event_at = excluded.last_event_at,
            last_heartbeat_at = excluded.last_heartbeat_at,
            updated_at = excluded.updated_at
        """,
        (timepoint, now, now, now),
    )


def request_json(path: str) -> dict[str, Any]:
    if not REST_KEY:
        raise RuntimeError("COMPANIES_HOUSE_REST_API_KEY is not configured")
    response = requests.get(
        f"{REST_BASE_URL}{path}",
        auth=(REST_KEY, ""),
        headers={"Accept": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def restricted_evidence(company_number: str) -> dict[str, Any]:
    profile = request_json(f"/company/{company_number}")
    officers = request_json(f"/company/{company_number}/officers")
    pscs = request_json(f"/company/{company_number}/persons-with-significant-control")

    directors: list[dict[str, Any]] = []
    for officer in officers.get("items") or []:
        role = normalize(officer.get("officer_role"))
        address = officer.get("address") or {}
        country = normalize(address.get("country"))
        if role in {"director", "corporate-director"} and any(
            country == normalize(target) or normalize(target) in country
            for target in TARGET_COUNTRIES
        ):
            directors.append({
                "name": officer.get("name"),
                "role": officer.get("officer_role"),
                "country": address.get("country"),
                "appointed_on": officer.get("appointed_on"),
            })

    corporate_pscs: list[dict[str, Any]] = []
    for psc in pscs.get("items") or []:
        kind = normalize(psc.get("kind"))
        if "corporate" in kind or "legal entity" in kind:
            corporate_pscs.append({
                "name": psc.get("name"),
                "kind": psc.get("kind"),
                "natures_of_control": psc.get("natures_of_control") or [],
            })

    return {
        "profile": profile,
        "directors": directors,
        "corporate_pscs": corporate_pscs,
        "qualified": bool(directors or corporate_pscs),
    }


def process_event(payload: dict[str, Any], event_hash: str) -> None:
    event = payload.get("event") or {}
    data = payload.get("data") or {}
    company_number = payload.get("resource_id") or data.get("company_number")
    if not company_number:
        return

    received = datetime.utcnow()
    db_write(
        "insert into public.raw_events(event_type, company_number, payload, received_at) values (%s, %s, %s, %s)",
        (event.get("type"), company_number, json.dumps(payload, default=str), received),
    )

    name = data.get("company_name") or company_number
    creation = parse_date(data.get("date_of_creation"))
    if creation != uk_today():
        return

    sics = data.get("sic_codes") or []
    target_sics = matching_sics(sics, TARGET_SIC_CODES)
    restricted_sics = matching_sics(sics, RESTRICTED_SIC_CODES)
    buzzword_matches = token_matches(name, BUZZWORDS)
    if not (target_sics or restricted_sics or buzzword_matches):
        return

    restricted = bool(restricted_sics)
    qualified = not restricted
    evidence: dict[str, Any] = {}
    enrichment_status = "not_required"
    lead_status = "qualified"

    if restricted:
        enrichment_status = "pending"
        lead_status = "enrichment_pending"
        if ENRICHMENT_ENABLED:
            evidence = restricted_evidence(company_number)
            qualified = bool(evidence.get("qualified"))
            enrichment_status = "complete"
            lead_status = "qualified" if qualified else "not_qualified"

    db_write(
        """
        insert into public.companies
            (company_number, company_name, date_of_creation, sic_codes, raw_data,
             first_seen_at, last_seen_at, enrichment_status, is_lead, lead_status,
             matched_buzzwords, matched_sic_codes, incorporated_today, last_screened_at)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, true, %s)
        on conflict (company_number) do update set
            company_name = excluded.company_name,
            date_of_creation = excluded.date_of_creation,
            sic_codes = excluded.sic_codes,
            raw_data = excluded.raw_data,
            last_seen_at = excluded.last_seen_at,
            enrichment_status = excluded.enrichment_status,
            is_lead = excluded.is_lead,
            lead_status = excluded.lead_status,
            matched_buzzwords = excluded.matched_buzzwords,
            matched_sic_codes = excluded.matched_sic_codes,
            incorporated_today = true,
            last_screened_at = excluded.last_screened_at
        """,
        (
            company_number, name, creation, json.dumps(sics),
            json.dumps({"stream": data, "evidence": evidence}, default=str),
            received, received, enrichment_status, qualified, lead_status,
            json.dumps(buzzword_matches), json.dumps(sorted(set(target_sics + restricted_sics))), received,
        ),
    )

    if restricted and not ENRICHMENT_ENABLED:
        db_write(
            "insert into public.enrichment_jobs(company_number, enrichment_scope) values (%s, 'initial_rest') on conflict (company_number, enrichment_scope) do nothing",
            (company_number,),
        )


def stream_loop() -> None:
    if not STREAM_ENABLED:
        update_status("disabled", "STREAM_ENABLED is false")
        return
    if not STREAM_KEY:
        update_status("degraded", "COMPANIES_HOUSE_STREAM_API_KEY is not configured")
        return

    backoff = 5
    while not _stop_event.is_set():
        try:
            update_status("connecting")
            checkpoint = get_checkpoint()
            params = {} if checkpoint is None else {"timepoint": checkpoint}
            with requests.get(STREAM_URL, params=params, auth=(STREAM_KEY, ""), headers={"Accept": "application/json"}, stream=True, timeout=(30, 300)) as response:
                response.raise_for_status()
                update_status("connected")
                for line in response.iter_lines(decode_unicode=True):
                    if _stop_event.is_set():
                        return
                    if not line:
                        continue
                    payload = json.loads(line)
                    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
                    process_event(payload, digest)
                    event = payload.get("event") or {}
                    if event.get("timepoint") is not None:
                        save_checkpoint(int(event["timepoint"]))
                    update_status("connected")
                backoff = 5
        except Exception as exc:
            logger.exception("Stream failure")
            try:
                update_status("degraded", f"{type(exc).__name__}: {exc}"[:1000])
            except Exception:
                pass
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)


def start_worker() -> bool:
    global _worker_thread
    with _worker_lock:
        if _worker_thread and _worker_thread.is_alive():
            return False
        _stop_event.clear()
        _worker_thread = threading.Thread(target=stream_loop, name=WORKER_NAME, daemon=True)
        _worker_thread.start()
        return True


def health() -> tuple[str, dict[str, Any] | None]:
    try:
        row = db_fetch_one("select * from public.worker_status where worker_name = %s", (WORKER_NAME,))
    except Exception as exc:
        return f"Database error: {exc}", None
    if not row:
        return "Not started", None
    return str(row.get("status") or "Unknown"), row


def dashboard() -> None:
    st.title("Thanos dashboard")
    try:
        with db_connection() as conn:
            conn.execute("select 1").fetchone()
        db_ok, db_error = True, ""
    except Exception as exc:
        db_ok, db_error = False, str(exc)

    worker_status, worker_row = health()
    c1, c2, c3 = st.columns(3)
    c1.metric("Database", "Connected" if db_ok else "Not connected")
    c2.metric("Worker", worker_status)
    c3.metric("Events committed", worker_row.get("events_committed", 0) if worker_row else 0)
    if db_error:
        st.error(db_error)
    if worker_row and worker_row.get("last_error"):
        st.warning(worker_row["last_error"])

    if st.button("Start worker", type="primary"):
        st.success("Worker started") if start_worker() else st.info("Worker is already running")
    if st.button("Refresh"):
        st.rerun()

    try:
        rows = db_fetch_all("select * from public.qualifying_leads order by first_seen_at desc")
        st.subheader("Qualifying companies")
        st.dataframe(rows, use_container_width=True)
    except Exception as exc:
        st.error(str(exc))


if __name__ == "__main__":
    dashboard()
