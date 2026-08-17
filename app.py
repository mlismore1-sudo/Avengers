from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Iterable

import psycopg
import requests
import streamlit as st
from psycopg.rows import dict_row

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("thanos")

WORKER_NAME = "company_stream_worker"
ENRICHMENT_WORKER_NAME = "restricted_enrichment_worker"
POLL_SECONDS = int(os.getenv("ENRICHMENT_POLL_SECONDS", "10"))
REST_BASE_URL = os.getenv("REST_BASE_URL", "https://api.company-information.service.gov.uk").rstrip("/")

_worker_lock = threading.Lock()
_worker_thread: threading.Thread | None = None
_enrichment_thread: threading.Thread | None = None
_stop_event = threading.Event()


def setting(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value:
        return value
    try:
        value = st.secrets.get(name)
    except Exception:
        value = None
    return str(value) if value is not None else default


def db_connection() -> psycopg.Connection:
    url = setting("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg.connect(url, row_factory=dict_row, connect_timeout=15)


def db_fetch_one(sql: str, params: Iterable[Any] | None = None) -> dict[str, Any] | None:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


def db_write(sql: str, params: Iterable[Any] | None = None) -> None:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()


def worker_status(worker_name: str, status: str, error: str | None = None) -> None:
    now = datetime.now(timezone.utc)
    db_write("""
        insert into public.worker_status
            (worker_name, process_id, status, heartbeat_at, last_error, updated_at)
        values (%s, %s, %s, %s, %s, %s)
        on conflict (worker_name) do update set
            process_id = excluded.process_id,
            status = excluded.status,
            heartbeat_at = excluded.heartbeat_at,
            last_error = excluded.last_error,
            updated_at = excluded.updated_at
    """, (worker_name, str(os.getpid()), status, now, error, now))


def claim_enrichment_job() -> dict[str, Any] | None:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                with next_job as (
                    select id
                    from public.enrichment_jobs
                    where status in ('pending', 'failed')
                      and (next_attempt_at is null or next_attempt_at <= now())
                    order by created_at
                    for update skip locked
                    limit 1
                )
                update public.enrichment_jobs job
                set status = 'processing',
                    attempts = attempts + 1,
                    updated_at = now(),
                    last_error = null
                from next_job
                where job.id = next_job.id
                returning job.*
            """)
            job = cur.fetchone()
        conn.commit()
    return job


def companies_house_json(path: str) -> dict[str, Any]:
    key = setting("COMPANIES_HOUSE_REST_API_KEY")
    if not key:
        raise RuntimeError("COMPANIES_HOUSE_REST_API_KEY is not configured")
    response = requests.get(
        f"{REST_BASE_URL}{path}",
        auth=(key, ""),
        headers={"Accept": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def enrich_restricted_company(company_number: str) -> dict[str, Any]:
    profile = companies_house_json(f"/company/{company_number}")
    officers = companies_house_json(f"/company/{company_number}/officers")
    pscs = companies_house_json(f"/company/{company_number}/persons-with-significant-control")

    target_countries = ("eu", "eea", "usa", "india")
    directors = []
    for officer in officers.get("items") or []:
        address = officer.get("address") or {}
        country = str(address.get("country") or "").lower()
        role = str(officer.get("officer_role") or "").lower()
        if role in {"director", "corporate-director"} and any(c in country for c in target_countries):
            directors.append({
                "name": officer.get("name"),
                "role": officer.get("officer_role"),
                "country": address.get("country"),
                "appointed_on": officer.get("appointed_on"),
            })

    corporate_pscs = []
    for psc in pscs.get("items") or []:
        kind = str(psc.get("kind") or "").lower()
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
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }


def complete_enrichment(job: dict[str, Any], evidence: dict[str, Any]) -> None:
    qualified = bool(evidence.get("qualified"))
    evidence_json = json.dumps(evidence, default=str)
    db_write("""
        update public.companies
        set qualification_evidence = %s::jsonb,
            enrichment_status = 'complete',
            enriched_at = now(),
            is_lead = %s,
            lead_status = %s,
            last_screened_at = now()
        where company_number = %s
    """, (evidence_json, qualified, "qualified" if qualified else "not_qualified", job["company_number"]))
    db_write("""
        update public.enrichment_jobs
        set status = 'complete', completed_at = now(), updated_at = now(), last_error = null
        where id = %s
    """, (job["id"],))


def fail_enrichment(job: dict[str, Any], exc: Exception) -> None:
    attempts = int(job.get("attempts") or 1)
    delay = min(3600, 30 * (2 ** max(0, attempts - 1)))
    message = f"{type(exc).__name__}: {exc}"[:2000]
    db_write("""
        update public.enrichment_jobs
        set status = 'failed',
            last_error = %s,
            next_attempt_at = now() + (%s * interval '1 second'),
            updated_at = now()
        where id = %s
    """, (message, delay, job["id"]))
    db_write("""
        update public.companies
        set enrichment_status = 'failed', last_screened_at = now()
        where company_number = %s
    """, (job["company_number"],))


def enrichment_loop() -> None:
    """Runs inside the Streamlit server process; Supabase is the queue and state store."""
    try:
        worker_status(ENRICHMENT_WORKER_NAME, "starting")
    except Exception:
        logger.exception("Could not write initial enrichment status")

    while not _stop_event.is_set():
        job = None
        try:
            job = claim_enrichment_job()
            if not job:
                worker_status(ENRICHMENT_WORKER_NAME, "idle")
                _stop_event.wait(POLL_SECONDS)
                continue
            worker_status(ENRICHMENT_WORKER_NAME, "enriching")
            evidence = enrich_restricted_company(job["company_number"])
            complete_enrichment(job, evidence)
            worker_status(ENRICHMENT_WORKER_NAME, "idle")
        except Exception as exc:
            logger.exception("Restricted enrichment failed")
            if job:
                try:
                    fail_enrichment(job, exc)
                except Exception:
                    logger.exception("Could not mark enrichment job failed")
            try:
                worker_status(ENRICHMENT_WORKER_NAME, "degraded", str(exc)[:1000])
            except Exception:
                pass
            _stop_event.wait(POLL_SECONDS)


def start_in_process_workers() -> None:
    global _worker_thread, _enrichment_thread
    with _worker_lock:
        if _enrichment_thread is None or not _enrichment_thread.is_alive():
            _enrichment_thread = threading.Thread(
                target=enrichment_loop,
                name=ENRICHMENT_WORKER_NAME,
                daemon=True,
            )
            _enrichment_thread.start()

        # Call the existing stream_loop from the existing app here.
        # Ensure it also uses the shared _stop_event and does not start twice.
        stream_loop_function = globals().get("stream_loop")
        if stream_loop_function and (_worker_thread is None or not _worker_thread.is_alive()):
            _worker_thread = threading.Thread(
                target=stream_loop_function,
                name=WORKER_NAME,
                daemon=True,
            )
            _worker_thread.start()


def worker_state(worker_name: str) -> tuple[str, dict[str, Any] | None]:
    row = db_fetch_one(
        "select * from public.worker_status where worker_name = %s",
        (worker_name,),
    )
    if not row:
        return "Not started", None
    heartbeat = row.get("heartbeat_at")
    if heartbeat and heartbeat.tzinfo is None:
        heartbeat = heartbeat.replace(tzinfo=timezone.utc)
    if heartbeat and (datetime.now(timezone.utc) - heartbeat).total_seconds() > 60:
        return "Stale", row
    return str(row.get("status") or "Unknown"), row


def status_indicator(label: str, active: bool) -> None:
    css = "active" if active else "inactive"
    text = "ACTIVE" if active else "INACTIVE"
    st.markdown(
        f'<div class="status-item"><span class="dot {css}"></span>'
        f'<span>{label}: <b>{text}</b></span></div>',
        unsafe_allow_html=True,
    )


@st.fragment(run_every="15s")
def live_dashboard() -> None:
    start_in_process_workers()

    try:
        with db_connection() as conn:
            conn.execute("select 1").fetchone()
        database_ok = True
        database_error = ""
    except Exception as exc:
        database_ok = False
        database_error = str(exc)

    try:
        stream_status, stream_row = worker_state(WORKER_NAME)
        enrichment_status, enrichment_row = worker_state(ENRICHMENT_WORKER_NAME)
    except Exception as exc:
        stream_status, stream_row = "Database error", None
        enrichment_status, enrichment_row = "Database error", None
        database_error = str(exc)
        database_ok = False

    st.markdown('<div class="status-bar">', unsafe_allow_html=True)
    status_indicator("Database", database_ok)
    status_indicator("Company stream", stream_status in {"connected", "connecting"})
    status_indicator("Restricted enrichment", enrichment_status in {"starting", "enriching", "idle"})
    st.markdown("</div>", unsafe_allow_html=True)

    if database_error:
        st.error(database_error)
    if stream_row and stream_row.get("last_error"):
        st.warning(f"Stream worker: {stream_row['last_error']}")
    if enrichment_row and enrichment_row.get("last_error"):
        st.warning(f"Enrichment worker: {enrichment_row['last_error']}")

    st.caption(
        f"Company stream: {stream_status} · Restricted enrichment: {enrichment_status} "
        "· Refreshing every 15 seconds"
    )


def main() -> None:
    st.set_page_config(page_title="Thanos dashboard", page_icon="🟢", layout="wide")
    st.markdown("""<style>
    .status-bar{display:flex;gap:2rem;padding:1rem;border:1px solid #333;border-radius:10px;margin-bottom:1rem}
    .status-item{display:flex;align-items:center;gap:.5rem;font-size:1.05rem}
    .dot{width:13px;height:13px;border-radius:50%;display:inline-block}
    .active{background:#18c964;box-shadow:0 0 8px #18c964;animation:pulse 1.2s infinite}
    .inactive{background:#777}
    @keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
    </style>""", unsafe_allow_html=True)
    st.title("Thanos dashboard")
    live_dashboard()


if __name__ == "__main__":
    main()
