#!/usr/bin/env python3
"""
queuectl.py

Full, single-file, production-like implementation of the assignment requirements.
This is intentionally verbose and complete so you have "the whole code" in one file.
Features:
 - SQLite persistence (jobs + config)
 - CLI: enqueue, worker start/stop, status, list, dlq list/retry, config set/show
 - Multiple worker processes (multiprocessing)
 - Exponential backoff retry
 - Dead Letter Queue (state == 'dead')
 - Graceful shutdown (SIGINT/SIGTERM)
 - Simple locking to avoid duplicate work (SQLite row update claim)
 - Logging to both console and a log file
 - Useful helper commands and helpful messages for recording/demo

Drop this file into your project folder and run with:
    python queuectl.py -h
"""

from __future__ import annotations
import argparse
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
import signal
import logging
from datetime import datetime, timezone
from multiprocessing import Process, Event, current_process, Manager

# -----------------------
# Configuration / paths
# -----------------------
DB_PATH = os.environ.get("QUEUECTL_DB", "queuectl.db")
WORKER_CONTROL_FILE = os.environ.get("QUEUECTL_CONTROL", "queuectl.workers.flag")
LOG_FILE = os.environ.get("QUEUECTL_LOG", "queuectl.log")

DEFAULT_BACKOFF_BASE = 2
DEFAULT_MAX_RETRIES = 3
WORKER_POLL_INTERVAL = 0.5  # seconds

# -----------------------
# Logging setup
# -----------------------
logger = logging.getLogger("queuectl")
logger.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s %(levelname)s [%(processName)s] %(message)s")
# console handler
ch = logging.StreamHandler()
ch.setFormatter(fmt)
ch.setLevel(logging.INFO)
logger.addHandler(ch)
# file handler
fh = logging.FileHandler(LOG_FILE)
fh.setFormatter(fmt)
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

# -----------------------
# Utility helpers
# -----------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def ensure_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()
    # jobs table
    cur.execute("""
      CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL,
        available_at REAL NOT NULL DEFAULT 0,
        created_at TEXT,
        updated_at TEXT,
        last_error TEXT,
        worker TEXT
      )
    """)
    # config table
    cur.execute("""
      CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT
      )
    """)
    # initialize defaults
    cur.execute("INSERT OR IGNORE INTO config(key, value) VALUES ('backoff_base', ?)", (str(DEFAULT_BACKOFF_BASE),))
    cur.execute("INSERT OR IGNORE INTO config(key, value) VALUES ('default_max_retries', ?)", (str(DEFAULT_MAX_RETRIES),))
    conn.commit()
    conn.close()

def get_config_value(key: str, default=None):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()
    cur.execute("SELECT value FROM config WHERE key = ?", (key,))
    r = cur.fetchone()
    conn.close()
    if r:
        return r[0]
    return default

def set_config_value(key: str, value: str):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO config(key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()

# -----------------------
# Job store (SQLite)
# -----------------------
class JobStore:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        ensure_db()

    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def add_job(self, job_id: str, command: str, max_retries: int = None) -> None:
        if max_retries is None:
            max_retries = int(get_config_value("default_max_retries", str(DEFAULT_MAX_RETRIES)))
        now = now_iso()
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO jobs(id, command, state, attempts, max_retries, available_at, created_at, updated_at)
            VALUES (?, ?, 'pending', 0, ?, 0, ?, ?)
        """, (job_id, command, int(max_retries), now, now))
        conn.commit()
        conn.close()
        logger.info("Enqueued job %s", job_id)

    def list_jobs(self, state: str = None, limit: int = 100):
        conn = self._conn()
        cur = conn.cursor()
        if state:
            cur.execute("SELECT * FROM jobs WHERE state = ? ORDER BY created_at LIMIT ?", (state, limit))
        else:
            cur.execute("SELECT * FROM jobs ORDER BY created_at LIMIT ?", (limit,))
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_job(self, job_id: str):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        r = cur.fetchone()
        conn.close()
        return dict(r) if r else None

    def update_job_state(self, job_id: str, state: str, **kwargs):
        conn = self._conn()
        cur = conn.cursor()
        fields = ["state = ?"]
        params = [state]
        for k, v in kwargs.items():
            fields.append(f"{k} = ?")
            params.append(v)
        params.append(job_id)
        sql = f"UPDATE jobs SET {', '.join(fields)}, updated_at = ? WHERE id = ?"
        params.insert(-1, now_iso())  # updated_at before job_id
        cur.execute(sql, params)
        conn.commit()
        conn.close()

    def claim_job_atomically(self):
        """
        Picks a job which is pending or failed and available_at <= now,
        and atomically sets state=processing and assigns worker. Returns job dict or None.
        """
        conn = self._conn()
        cur = conn.cursor()
        now_ts = time.time()
        cur.execute("""
            SELECT * FROM jobs
            WHERE state IN ('pending','failed') AND available_at <= ?
            ORDER BY created_at LIMIT 1
        """, (now_ts,))
        r = cur.fetchone()
        if not r:
            conn.close()
            return None
        job = dict(r)
        jid = job["id"]
        # try to claim
        cur2 = conn.cursor()
        cur2.execute("""
            UPDATE jobs SET state='processing', worker=? , updated_at=? 
            WHERE id = ? AND state IN ('pending','failed') AND available_at <= ?
        """, (current_process().name, now_iso(), jid, now_ts))
        if cur2.rowcount == 1:
            conn.commit()
            cur3 = conn.cursor()
            cur3.execute("SELECT * FROM jobs WHERE id = ?", (jid,))
            res = cur3.fetchone()
            conn.close()
            return dict(res) if res else None
        conn.commit()
        conn.close()
        return None

    def set_job_failed(self, jid: str, attempts: int, available_at: float, last_error: str):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE jobs SET state='failed', attempts=?, available_at=?, last_error=?, updated_at=?, worker=NULL
            WHERE id = ?
        """, (attempts, available_at, last_error, now_iso(), jid))
        conn.commit()
        conn.close()

    def set_job_completed(self, jid: str):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("UPDATE jobs SET state='completed', updated_at=?, worker=NULL WHERE id = ?", (now_iso(), jid))
        conn.commit()
        conn.close()

    def set_job_dead(self, jid: str, attempts: int, last_error: str):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("UPDATE jobs SET state='dead', attempts=?, last_error=?, updated_at=?, worker=NULL WHERE id = ?",
                    (attempts, last_error, now_iso(), jid))
        conn.commit()
        conn.close()

    def retry_dead_to_pending(self, jid: str):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM jobs WHERE id = ? AND state = 'dead'", (jid,))
        if not cur.fetchone():
            conn.close()
            return False
        cur.execute("UPDATE jobs SET state='pending', attempts=0, available_at=0, updated_at=?, last_error=NULL WHERE id = ?",
                    (now_iso(), jid))
        conn.commit()
        conn.close()
        return True

# -----------------------
# Worker logic (process)
# -----------------------
def run_shell_command(cmd: str):
    try:
        proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return proc.returncode, proc.stdout, proc.stderr
    except Exception as e:
        return 1, "", str(e)

def worker_loop(stop_event: Event):
    store = JobStore(DB_PATH)
    while not stop_event.is_set():
        try:
            job = store.claim_job_atomically()
            if not job:
                time.sleep(WORKER_POLL_INTERVAL)
                continue
            jid = job["id"]
            logger.info("Picked job %s", jid)
            rc, out, err = run_shell_command(job["command"])
            if rc == 0:
                store.set_job_completed(jid)
                logger.info("Job %s completed", jid)
            else:
                attempts = job["attempts"] + 1
                maxr = job["max_retries"]
                backoff = int(get_config_value("backoff_base", str(DEFAULT_BACKOFF_BASE)))
                delay = backoff ** attempts
                available_at = time.time() + delay
                last_error = f"rc={rc} err={(err or '')[:200]}"
                if attempts > maxr:
                    store.set_job_dead(jid, attempts, last_error)
                    logger.warning("Job %s moved to DLQ (dead).", jid)
                else:
                    store.set_job_failed(jid, attempts, available_at, last_error)
                    logger.warning("Job %s failed; retry in %ds (attempt %d/%d).", jid, delay, attempts, maxr)
        except sqlite3.OperationalError as e:
            logger.debug("OperationalError in worker loop: %s", e)
            time.sleep(0.5)
        except Exception as e:
            logger.exception("Unhandled exception in worker loop: %s", e)
            time.sleep(1)
    logger.info("Worker loop exiting")

# -----------------------
# Worker process manager
# -----------------------
_worker_processes: list[Process] = []

def start_workers(count: int):
    ensure_db()
    # create control file to indicate running
    with open(WORKER_CONTROL_FILE, "w") as f:
        f.write("running")
    global _worker_processes
    _worker_processes = []
    for i in range(count):
        p = Process(target=worker_main_process, args=(WORKER_CONTROL_FILE,), name=f"worker-{i+1}")
        p.start()
        _worker_processes.append(p)
    logger.info("Started %d workers. PIDs: %s", len(_worker_processes), [p.pid for p in _worker_processes])
    print(f"Started {len(_worker_processes)} workers. (pids: {[p.pid for p in _worker_processes]})")

def stop_workers():
    if os.path.exists(WORKER_CONTROL_FILE):
        os.remove(WORKER_CONTROL_FILE)
        logger.info("Requested workers to stop (control file removed)")
        print("Requested workers to stop. They will exit after current job.")
    else:
        print("No worker control file present. No running workers detected.")

def worker_main_process(control_file_path: str):
    # Setup SIGTERM handler
    stop_flag = threading.Event()
    def _term(signum, frame):
        stop_flag.set()
    signal.signal(signal.SIGTERM, _term)
    logger.info("Worker process %s starting (pid=%d)", current_process().name, os.getpid())
    store = JobStore(DB_PATH)
    while True:
        if not os.path.exists(control_file_path):
            logger.info("Control file removed; worker %s exiting", current_process().name)
            break
        try:
            job = store.claim_job_atomically()
            if not job:
                time.sleep(WORKER_POLL_INTERVAL)
                if stop_flag.is_set():
                    logger.info("Worker %s received stop signal", current_process().name)
                    break
                continue
            jid = job["id"]
            logger.info("[%s] picked %s", current_process().name, jid)
            rc, out, err = run_shell_command(job["command"])
            if rc == 0:
                store.set_job_completed(jid)
                logger.info("[%s] completed %s", current_process().name, jid)
            else:
                attempts = job["attempts"] + 1
                maxr = job["max_retries"]
                backoff = int(get_config_value("backoff_base", str(DEFAULT_BACKOFF_BASE)))
                delay = backoff ** attempts
                available_at = time.time() + delay
                last_error = f"rc={rc} err={(err or '')[:200]}"
                if attempts > maxr:
                    store.set_job_dead(jid, attempts, last_error)
                    logger.warning("[%s] job %s moved to DLQ", current_process().name, jid)
                else:
                    store.set_job_failed(jid, attempts, available_at, last_error)
                    logger.warning("[%s] job %s failed; retry in %ds (attempt %d/%d)", current_process().name, jid, delay, attempts, maxr)
        except sqlite3.OperationalError as e:
            logger.debug("[%s] sqlite op error: %s", current_process().name, e)
            time.sleep(0.3)
        except Exception as e:
            logger.exception("[%s] worker exception: %s", current_process().name, e)
            time.sleep(0.5)
        if stop_flag.is_set():
            logger.info("Worker %s received stop signal (sigterm)", current_process().name)
            break
    logger.info("Worker %s exit", current_process().name)

# -----------------------
# DLQ operations
# -----------------------
def dlq_list():
    ensure_db()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE state = 'dead' ORDER BY updated_at")
    rows = cur.fetchall()
    conn.close()
    for r in rows:
        print(json.dumps(dict(r), default=str))

def dlq_retry(job_id: str):
    ensure_db()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id = ? AND state = 'dead'", (job_id,))
    r = cur.fetchone()
    if not r:
        print("No dead job with id", job_id)
        conn.close()
        return
    now = now_iso()
    cur.execute("UPDATE jobs SET state='pending', attempts=0, available_at=0, updated_at=?, last_error=NULL WHERE id = ?",
                (now, job_id))
    conn.commit()
    conn.close()
    print("Retried job", job_id)

# -----------------------
# Enqueue / List / Status
# -----------------------
def enqueue_from_json(json_str: str):
    ensure_db()
    try:
        obj = json.loads(json_str)
    except Exception as e:
        raise ValueError("Invalid JSON: " + str(e))
    jid = obj.get("id") or str(uuid.uuid4())
    cmd = obj.get("command")
    if not cmd:
        raise ValueError("job must include 'command'")
    max_retries = obj.get("max_retries")
    if max_retries is None:
        max_retries = int(get_config_value("default_max_retries", str(DEFAULT_MAX_RETRIES)))
    store = JobStore(DB_PATH)
    store.add_job(jid, cmd, int(max_retries))
    print("enqueued", jid)

def list_jobs_cli(state: str | None = None):
    ensure_db()
    store = JobStore(DB_PATH)
    rows = store.list_jobs(state=state)
    for r in rows:
        print(json.dumps(r, default=str))

def status_cli():
    ensure_db()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()
    cur.execute("SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state")
    rows = cur.fetchall()
    counts = {r[0]: r[1] for r in rows}
    backoff = get_config_value("backoff_base", str(DEFAULT_BACKOFF_BASE))
    default_max = get_config_value("default_max_retries", str(DEFAULT_MAX_RETRIES))
    print("Job counts:", counts)
    print("Config: backoff_base=", backoff, "default_max_retries=", default_max)
    conn.close()

def delete_job(job_id: str):
    ensure_db()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()
    cur.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    conn.commit()
    deleted = cur.rowcount
    conn.close()
    if deleted:
        print(f"Deleted job {job_id}")
    else:
        print(f"No job found with id {job_id}")

# -----------------------
# CLI and argument parsing
# -----------------------
def parse_args():
    p = argparse.ArgumentParser(prog="queuectl", description="queuectl - production-like job queue")
    sub = p.add_subparsers(dest="cmd")

    e = sub.add_parser("enqueue", help="enqueue a job by passing JSON object (single arg)")
    e.add_argument("jobjson", help='json string for job, e.g. \'{"id":"job1","command":"sleep 2"}\'')

    w = sub.add_parser("worker", help="worker controls")
    wsub = w.add_subparsers(dest="wcmd")
    wstart = wsub.add_parser("start", help="start worker processes")
    wstart.add_argument("--count", type=int, default=1)
    wsub.add_parser("stop", help="stop workers gracefully")

    s = sub.add_parser("status", help="show status summary")

    l = sub.add_parser("list", help="list jobs")
    l.add_argument("--state", choices=["pending","processing","completed","failed","dead"], default=None)

    d = sub.add_parser("dlq", help="DLQ operations")
    dsub = d.add_subparsers(dest="dcmd")
    dsub.add_parser("list", help="list dead jobs")
    dretry = dsub.add_parser("retry", help="retry a dead job")
    dretry.add_argument("jobid")

    c = sub.add_parser("config", help="configuration")
    csub = c.add_subparsers(dest="ccmd")
    cset = csub.add_parser("set", help="set config key")
    cset.add_argument("key")
    cset.add_argument("value")
    csub.add_parser("show", help="show config")

    # ✅ Add delete command here
    delcmd = sub.add_parser("delete", help="delete a job by id")
    delcmd.add_argument("jobid", help="ID of job to delete")

    return p.parse_args()


def main():
    args = parse_args()
    ensure_db()

    if args.cmd == "enqueue":
        enqueue_from_json(args.jobjson)

    elif args.cmd == "worker":
        if args.wcmd == "start":
            start_workers(args.count)
        elif args.wcmd == "stop":
            stop_workers()
        else:
            print("unknown worker subcommand")

    elif args.cmd == "status":
        status_cli()

    elif args.cmd == "list":
        list_jobs_cli(state=args.state)

    elif args.cmd == "dlq":
        if args.dcmd == "list":
            dlq_list()
        elif args.dcmd == "retry":
            dlq_retry(args.jobid)
        else:
            print("unknown dlq subcommand")

    elif args.cmd == "config":
        if args.ccmd == "set":
            set_config_value(args.key, args.value)
            print("config set")
        elif args.ccmd == "show":
            cfg = {
                "backoff_base": get_config_value("backoff_base", str(DEFAULT_BACKOFF_BASE)),
                "default_max_retries": get_config_value("default_max_retries", str(DEFAULT_MAX_RETRIES))
            }
            print(json.dumps(cfg, indent=2))
        else:
            print("unknown config subcommand")

    elif args.cmd == "delete":
        delete_job(args.jobid)

    else:
        print("No command specified. Use -h for help.")

if __name__ == "__main__":
    main()