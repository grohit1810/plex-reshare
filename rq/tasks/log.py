"""Structured logging for plex-reshare.

Goal: logs that are easy to eyeball AND easy to grep/parse when shared for
remote diagnosis. Every line is:

    2026-07-02 15:30:12  INFO   component      key=value key=value ...

- Fixed columns (time, level, component) so `grep`/`awk` work cleanly.
- key=value payloads so counts/timings/decisions are machine-readable.
- Always to stdout -> captured by supervisord -> `docker compose logs`.
- Optionally ALSO to a rotating file (set LOG_FILE), handy for sharing logs.

Env knobs:
  LOG_LEVEL  DEBUG|INFO|WARNING  (default INFO)
  LOG_FILE   path to also write logs to, e.g. /pr/plex-reshare.log (default: off).
             Put it under /pr so it lands on the host volume and survives restarts.
  LOG_FILE_MAX_MB  per-file size cap before rotation (default 10)
  LOG_FILE_BACKUPS number of rotated files kept (default 3)
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler

_CONFIGURED = False
_FORMATTER = logging.Formatter(
    fmt="%(asctime)s  %(levelname)-5s  %(name)-16s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def setup() -> None:
    """Configure root logging once. Safe to call from every process/worker boot."""
    global _CONFIGURED
    if _CONFIGURED:
        return
    level = os.getenv("LOG_LEVEL", "INFO").upper()

    handlers = [logging.StreamHandler(sys.stdout)]

    # optional rotating file (e.g. LOG_FILE=/pr/plex-reshare.log). Rotation caps disk
    # use; the container has multiple processes (worker/starlette) each running setup(),
    # but RotatingFileHandler shares the OS file so appends interleave safely.
    log_file = os.getenv("LOG_FILE", "").strip()
    if log_file:
        try:
            max_bytes = int(os.getenv("LOG_FILE_MAX_MB", "10")) * 1024 * 1024
            backups = int(os.getenv("LOG_FILE_BACKUPS", "3"))
            handlers.append(RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backups))
        except OSError as e:
            # never let a bad LOG_FILE path kill logging -- fall back to stdout only
            sys.stderr.write(f"[log] could not open LOG_FILE {log_file!r}: {e}\n")

    for h in handlers:
        h.setFormatter(_FORMATTER)

    root = logging.getLogger("pr")
    root.handlers[:] = handlers
    root.setLevel(getattr(logging, level, logging.INFO))
    root.propagate = False
    _CONFIGURED = True


def get(component: str) -> logging.Logger:
    """Return the logger for a component, e.g. get('crawl.movies'). Namespaced under
    'pr.' so all our logs share a prefix and can be filtered as a group."""
    setup()
    return logging.getLogger(f"pr.{component}")


def kv(**fields) -> str:
    """Render key=value pairs in a stable order for a log message. Strings with
    spaces are quoted so each pair stays one token for grep/awk."""
    parts = []
    for k, v in fields.items():
        if isinstance(v, str) and (" " in v or v == ""):
            v = f'"{v}"'
        parts.append(f"{k}={v}")
    return " ".join(parts)
