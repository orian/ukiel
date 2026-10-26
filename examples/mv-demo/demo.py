#!/usr/bin/env python3
"""Run and query the local orders -> sales MV demo (Python 3.11+, no pip)."""

import argparse
from collections import Counter
import errno
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.request
import uuid


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
STATE = HERE / ".state"
CONFIG = tomllib.loads((HERE / "ukield.toml").read_text())
BASE = "http://" + CONFIG["query"]["listen"]
COMPOSE = ["docker", "compose", "--project-name", "ukiel-mv-demo", "--file", str(HERE / "compose.yaml")]
TOPIC = CONFIG["tables"][0]["topic"]
RAW_TOTALS = """SELECT product, COUNT(*) AS orders, SUM(quantity) AS units,
    SUM(amount_cents) AS revenue_cents FROM orders GROUP BY product ORDER BY product"""
MV_TOTALS = """SELECT product, SUM(orders) AS orders, SUM(units) AS units,
    SUM(revenue_cents) AS revenue_cents FROM sales GROUP BY product ORDER BY product"""


def say(message):
    print(message, flush=True)


def run(command, **kwargs):
    return subprocess.run(command, cwd=ROOT, check=True, text=True, **kwargs)


def http(path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(BASE + path, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"HTTP {error.code}: {error.read().decode()}") from error


def query(sql, namespace=1):
    return http("/api/query", {"namespace_id": namespace, "sql": sql, "format": "rows"})


def count(namespace):
    return query("SELECT COUNT(*) AS n FROM orders", namespace)["rows"][0]["n"]


def wait_for(description, predicate, process=None, timeout=60):
    deadline = time.monotonic() + timeout
    last_error = "condition not yet satisfied"
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"ukield exited with code {process.returncode}")
        try:
            if predicate():
                return
        except (OSError, RuntimeError) as error:
            last_error = str(error)
        time.sleep(0.2)
    raise RuntimeError(f"Timed out waiting for {description}: {last_error}")


def check():
    def converged():
        for namespace in (1, 2):
            raw = query(RAW_TOTALS, namespace)["rows"]
            materialized = query(MV_TOTALS, namespace)["rows"]
            if raw != materialized:
                return False
        return True

    wait_for("MV totals to match raw orders for both tenants", converged)
    say(f"Verified: raw orders and MV totals agree (tenant 1: {count(1)} orders; tenant 2: {count(2)} orders).")


def seed(path):
    records = []
    now = time.time_ns() // 1_000_000
    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict) or type(row.get("tenant_id")) is not int or row["tenant_id"] not in (1, 2):
            raise ValueError(f"{path}:{line_number}: tenant_id must be 1 or 2")
        if not isinstance(row.get("product"), str) or any(type(row.get(name)) is not int for name in ("quantity", "amount_cents")):
            raise ValueError(f"{path}:{line_number}: product, quantity, and amount_cents are required")
        records.append({**row, "ts": now, "order_id": str(uuid.uuid4())})
    if not records:
        raise ValueError("sample file is empty")
    expected = Counter(row["tenant_id"] for row in records)
    before = {namespace: count(namespace) for namespace in expected}
    run(COMPOSE + ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
                   "--bootstrap-server", "kafka:9092", "--topic", TOPIC,
                   "--producer-property", "acks=all"],
        input="".join(json.dumps(row) + "\n" for row in records), timeout=60)
    wait_for("the sample orders to become queryable", lambda: all(
        count(namespace) >= before[namespace] + added for namespace, added in expected.items()))
    check()
    say(f"Added {len(records)} orders with fresh timestamps and order IDs.")


def ensure_port_available():
    host, port = CONFIG["query"]["listen"].rsplit(":", 1)
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind((host, int(port)))
        except OSError as error:
            if error.errno == errno.EADDRINUSE:
                raise RuntimeError(f"{BASE} is already in use; stop that server first") from error
            raise RuntimeError(f"Cannot bind {BASE}: {error}") from error


def stop():
    STATE.mkdir(parents=True, exist_ok=True)
    with (STATE / "run.lock").open("a") as lock:
        def stopped():
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                return False

        if not stopped():
            (STATE / "stop").touch()
            wait_for("the demo to stop", stopped, timeout=90)
        say("Demo stopped; saved data is retained.")


def start():
    for executable in ("cargo", "docker"):
        if shutil.which(executable) is None:
            raise RuntimeError(f"{executable} is required; see {HERE / 'README.md'}")
    ensure_port_available()
    run(["docker", "compose", "version"], stdout=subprocess.DEVNULL)
    run(["docker", "info"], stdout=subprocess.DEVNULL)
    (STATE / "stop").unlink(missing_ok=True)
    log_path = STATE / "ukield.log"
    log_path.unlink(missing_ok=True)
    say("Building ukield (the first build can take several minutes)...")
    run(["cargo", "build", "--locked", "-p", "ukield"])
    metadata = json.loads(run(["cargo", "metadata", "--no-deps", "--format-version=1"], capture_output=True).stdout)
    binary = Path(metadata["target_directory"]) / "debug" / "ukield"
    process = None
    try:
        say("Starting the demo's PostgreSQL, Kafka, and MinIO services...")
        run(COMPOSE + ["up", "--detach", "--wait", "--wait-timeout", "120"])
        run(COMPOSE + ["exec", "-T", "-e", "MC_HOST_demo=http://minioadmin:minioadmin@127.0.0.1:9000",
                       "minio", "mc", "mb", "--ignore-existing", "demo/ukiel-demo"])
        run(COMPOSE + ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
                       "--bootstrap-server", "kafka:9092", "--create", "--if-not-exists",
                       "--topic", TOPIC, "--partitions", "1", "--replication-factor", "1",
                       "--config", "message.timestamp.type=LogAppendTime"])
        env = os.environ.copy()
        # The example's explicit local endpoints must own its connections.
        for name in ("UKIELD_CATALOG_URL", "UKIELD_S3_ACCESS_KEY_ID", "UKIELD_S3_SECRET_ACCESS_KEY"):
            env.pop(name, None)
        env.setdefault("RUST_LOG", "warn,ukield=info")
        with log_path.open("w") as log:
            process = subprocess.Popen([str(binary), "--config", str(HERE / "ukield.toml")],
                                       cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=True)
        say("Waiting for ingest and MV readiness...")
        wait_for("all server roles to become ready", lambda: http("/readyz")["ready"], process)
        offsets = run(COMPOSE + ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                                 "--bootstrap-server", "kafka:9092", "--topic", TOPIC], capture_output=True).stdout
        end = sum(int(line.rsplit(":", 1)[1]) for line in offsets.splitlines() if line.startswith(TOPIC + ":"))
        if end == 0:
            seed(HERE / "orders.jsonl")
        else:
            wait_for("saved orders to be consumed", lambda: count(1) + count(2) >= end, process)
            check()
        say(f"\nDemo ready at {BASE}. MinIO console: http://127.0.0.1:29001")
        say('In another terminal: python3 examples/mv-demo/demo.py query "SELECT product, SUM(orders) AS orders FROM sales GROUP BY product"')
        say("More queries: examples/mv-demo/queries.sql. Ctrl-C or the stop command stops the demo and keeps its data.")
        while process.poll() is None and not (STATE / "stop").exists():
            time.sleep(0.2)
        if process.returncode:
            code = process.returncode
            raise RuntimeError(f"ukield exited with code {code}")
    except KeyboardInterrupt:
        say("\nStopping the demo...")
    except Exception:
        if log_path.exists():
            print("\nLast server log lines:\n" + "\n".join(log_path.read_text().splitlines()[-30:]), file=sys.stderr)
        raise
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        subprocess.run(COMPOSE + ["stop"], cwd=ROOT, check=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("start", help="start, seed, and run the demo until Ctrl-C")
    commands.add_parser("stop", help="gracefully stop the demo from another terminal")
    sql = commands.add_parser("query", help="execute one SQL query")
    sql.add_argument("sql")
    sql.add_argument("--namespace", type=int, choices=(1, 2), default=1)
    produce = commands.add_parser("seed", help="append another sample batch and verify its MV")
    produce.add_argument("--file", type=Path, default=HERE / "orders.jsonl")
    commands.add_parser("check", help="compare raw and materialized totals for both tenants")
    commands.add_parser("status", help="show readiness of all server roles")
    commands.add_parser("reset", help="delete this demo's containers and data after it stops")
    args = parser.parse_args()
    if args.command == "query":
        print(json.dumps(query(args.sql, args.namespace), indent=2))
    elif args.command == "seed":
        seed(args.file)
    elif args.command == "check":
        check()
    elif args.command == "status":
        print(json.dumps(http("/readyz"), indent=2))
    elif args.command == "stop":
        stop()
    else:
        STATE.mkdir(parents=True, exist_ok=True)
        with (STATE / "run.lock").open("w") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise RuntimeError("the demo is running; use Ctrl-C in its terminal first") from error
            if args.command == "start":
                def interrupt(_signal, _frame):
                    raise KeyboardInterrupt

                signal.signal(signal.SIGTERM, interrupt)
                signal.signal(signal.SIGHUP, interrupt)
                start()
            else:
                ensure_port_available()
                run(COMPOSE + ["down", "--volumes", "--remove-orphans"])
                (STATE / "ukield.log").unlink(missing_ok=True)
                say("Demo data removed. Run start to load a fresh sample.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        sys.exit(f"Error: {error}")
