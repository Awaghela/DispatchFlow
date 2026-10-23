#!/usr/bin/env python3
"""Run the whole DispatchFlow stack locally WITHOUT Docker (PostgreSQL, Redis, an SQS emulator,
API, worker) and start/stop/kill individual pieces. The load-test harness uses this to inject
service interruptions.

    python scripts/stackctl.py up            # start everything
    python scripts/stackctl.py status
    python scripts/stackctl.py stop worker --kill     # simulate a crash (SIGKILL)
    python scripts/stackctl.py start worker
    python scripts/stackctl.py restart redis
    python scripts/stackctl.py reset         # wipe DB + queues + cache
    python scripts/stackctl.py down

Requires postgresql + redis-server binaries on the machine (apt install postgresql redis-server).
"""
import argparse
import glob
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUN = ROOT / ".run"
LOGS = RUN / "logs"
IS_ROOT = os.geteuid() == 0
PGDATA = Path("/tmp/dispatchflow-pg") if IS_ROOT else RUN / "pgdata"  # postgres refuses to run as root
PG_PORT, REDIS_PORT, SQS_PORT = 5432, 6379, 5555
API_PORTS = {"api": 8000, "api2": 8001}

APP_ENV = {
    "DATABASE_URL": f"postgresql+asyncpg://dispatch:dispatch@localhost:{PG_PORT}/dispatchflow",
    "REDIS_URL": f"redis://localhost:{REDIS_PORT}/0",
    "AWS_ENDPOINT_URL": f"http://127.0.0.1:{SQS_PORT}",
    "AWS_ACCESS_KEY_ID": "test",
    "AWS_SECRET_ACCESS_KEY": "test",
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_REGION": "us-east-1",
    "SQS_AUTO_CREATE": "true",
    "SQS_VISIBILITY_TIMEOUT": "10",
    "SQS_MAX_RECEIVE_COUNT": "10",
    # test stack tuning: short retry horizon so "no courier available" failures are observable,
    # and fast self-healing of the Redis GEO index after an outage
    "MAX_ASSIGN_ATTEMPTS": "4",
    "ASSIGNMENT_TIMEOUT_SECONDS": "30",
    "REDRIVE_AFTER_SECONDS": "20",
    "GEO_RECONCILE_INTERVAL": "5",
    "LOG_LEVEL": "INFO",
}
SERVICES = ["postgres", "redis", "sqs", "worker", "api", "api2"]
DEFAULT_UP = ["postgres", "redis", "sqs", "worker", "api"]


def env_for_app() -> dict:
    env = {**os.environ, **APP_ENV}
    extra = RUN / "stack.env"  # optional KEY=VALUE overrides (e.g. CHAOS_WORKER_FAIL_RATE=0.05)
    if extra.exists():
        for line in extra.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def pg_bin(name: str) -> str:
    for d in sorted(glob.glob("/usr/lib/postgresql/*/bin"), reverse=True):
        if (Path(d) / name).exists():
            return str(Path(d) / name)
    found = shutil.which(name)
    if not found:
        sys.exit(f"{name} not found - install PostgreSQL")
    return found


def as_pg(cmd: list[str]) -> list[str]:
    return ["runuser", "-u", "postgres", "--", *cmd] if IS_ROOT else cmd


def pidfile(svc: str) -> Path:
    return RUN / f"{svc}.pid"


def read_pid(svc: str) -> int | None:
    try:
        pid = int(pidfile(svc).read_text())
        os.kill(pid, 0)
        return pid
    except (FileNotFoundError, ValueError, ProcessLookupError, PermissionError):
        return None


def is_up(svc: str) -> bool:
    if svc == "postgres":
        return subprocess.run([pg_bin("pg_isready"), "-h", "localhost", "-p", str(PG_PORT)], capture_output=True).returncode == 0
    return read_pid(svc) is not None


def wait_for(fn, what: str, timeout: float = 30) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            if fn():
                return
        except Exception:
            pass
        time.sleep(0.25)
    sys.exit(f"timed out waiting for {what}; see {LOGS}")


def spawn(svc: str, cmd: list[str], env: dict | None = None) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    log = open(LOGS / f"{svc}.log", "ab")
    p = subprocess.Popen(cmd, cwd=ROOT, env=env or os.environ, stdout=log, stderr=log, start_new_session=True)
    pidfile(svc).write_text(str(p.pid))


def http_ok(url: str) -> bool:
    import urllib.request

    try:
        return urllib.request.urlopen(url, timeout=1).status < 500
    except Exception:
        return False


# ----------------------------------------------------------------------------------------------
def start(svc: str) -> None:
    if is_up(svc):
        print(f"{svc}: already running")
        return
    RUN.mkdir(exist_ok=True)
    if svc == "postgres":
        if not (PGDATA / "PG_VERSION").exists():
            PGDATA.parent.mkdir(parents=True, exist_ok=True)
            if IS_ROOT:
                PGDATA.mkdir(exist_ok=True)
                shutil.chown(PGDATA, "postgres", "postgres")
            subprocess.run(as_pg([pg_bin("initdb"), "-D", str(PGDATA), "-U", "postgres", "--auth=trust", "-E", "UTF8"]), check=True, capture_output=True)
        opts = f"-p {PG_PORT} -k /tmp -c max_connections=300 -c shared_buffers=256MB -c listen_addresses=localhost"
        LOGS.mkdir(parents=True, exist_ok=True)
        pglog = Path("/tmp/dispatchflow-pg.log") if IS_ROOT else LOGS / "postgres.log"
        subprocess.run(as_pg([pg_bin("pg_ctl"), "-D", str(PGDATA), "-o", opts, "-l", str(pglog), "-w", "start"]), check=True, capture_output=True)
        wait_for(lambda: is_up("postgres"), "postgres")
        psql = as_pg([pg_bin("psql"), "-h", "/tmp", "-p", str(PG_PORT), "-U", "postgres", "-tAc"])
        if "1" not in subprocess.run([*psql, "SELECT 1 FROM pg_roles WHERE rolname='dispatch'"], capture_output=True, text=True).stdout:
            subprocess.run([*psql, "CREATE ROLE dispatch LOGIN PASSWORD 'dispatch' SUPERUSER"], check=True, capture_output=True)
        if "1" not in subprocess.run([*psql, "SELECT 1 FROM pg_database WHERE datname='dispatchflow'"], capture_output=True, text=True).stdout:
            subprocess.run([*psql, "CREATE DATABASE dispatchflow OWNER dispatch"], check=True, capture_output=True)
    elif svc == "redis":
        spawn("redis", ["redis-server", "--port", str(REDIS_PORT), "--save", "", "--appendonly", "no", "--bind", "127.0.0.1"])
        wait_for(lambda: subprocess.run(["redis-cli", "-p", str(REDIS_PORT), "ping"], capture_output=True, text=True).stdout.strip() == "PONG", "redis")
    elif svc == "sqs":
        spawn("sqs", [sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(SQS_PORT)])
        wait_for(lambda: http_ok(f"http://127.0.0.1:{SQS_PORT}/moto-api/"), "sqs emulator")
    elif svc == "worker":
        spawn("worker", [sys.executable, "-m", "app.worker"], env_for_app())
        time.sleep(1.0)
    elif svc in API_PORTS:
        port = API_PORTS[svc]
        spawn(svc, [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port), "--log-level", "warning", "--no-access-log"], env_for_app())
        wait_for(lambda: http_ok(f"http://127.0.0.1:{port}/health"), svc)
    else:
        sys.exit(f"unknown service {svc}")
    print(f"{svc}: started")


def stop(svc: str, kill: bool = False) -> None:
    if svc == "postgres":
        if is_up("postgres"):
            mode = "immediate" if kill else "fast"  # immediate == crash (WAL recovery on next start)
            subprocess.run(as_pg([pg_bin("pg_ctl"), "-D", str(PGDATA), "-m", mode, "-w", "stop"]), capture_output=True)
        print("postgres: stopped" + (" (crash)" if kill else ""))
        return
    pid = read_pid(svc)
    if pid is None:
        print(f"{svc}: not running")
        return
    try:
        os.killpg(pid, signal.SIGKILL if kill else signal.SIGTERM)
    except ProcessLookupError:
        pass
    for _ in range(100):
        if read_pid(svc) is None:
            break
        time.sleep(0.1)
    else:
        os.killpg(pid, signal.SIGKILL)
    pidfile(svc).unlink(missing_ok=True)
    print(f"{svc}: stopped" + (" (SIGKILL)" if kill else ""))


def reset() -> None:
    for svc in ("api2", "api", "worker"):
        stop(svc)
    stop("sqs")
    subprocess.run(["redis-cli", "-p", str(REDIS_PORT), "flushall"], capture_output=True)
    psql = as_pg([pg_bin("psql"), "-h", "/tmp", "-p", str(PG_PORT), "-U", "postgres", "-tAc"])
    subprocess.run([*psql, "DROP DATABASE IF EXISTS dispatchflow WITH (FORCE)"], check=True, capture_output=True)
    subprocess.run([*psql, "CREATE DATABASE dispatchflow OWNER dispatch"], check=True, capture_output=True)
    start("sqs")
    print("reset: database, cache and queues wiped (start worker/api again)")


def status() -> None:
    for svc in SERVICES:
        print(f"{svc:9s} {'UP' if is_up(svc) else 'down'}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["up", "down", "start", "stop", "restart", "status", "reset"])
    ap.add_argument("service", nargs="*")
    ap.add_argument("--kill", action="store_true", help="SIGKILL / immediate shutdown instead of a graceful stop")
    a = ap.parse_args()
    if a.cmd == "up":
        for svc in a.service or DEFAULT_UP:
            start(svc)
    elif a.cmd == "down":
        for svc in reversed(SERVICES):
            stop(svc)
    elif a.cmd == "start":
        for svc in a.service:
            start(svc)
    elif a.cmd == "stop":
        for svc in a.service:
            stop(svc, a.kill)
    elif a.cmd == "restart":
        for svc in a.service:
            stop(svc, a.kill)
            start(svc)
    elif a.cmd == "reset":
        reset()
    else:
        status()


if __name__ == "__main__":
    main()
