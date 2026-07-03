"""Targeted-scan IO layer: talks ONLY to the configured secondary Plex (PLEX_SCAN_URL)
and drives the per-folder scan drip. Pure helpers live in scan_paths; persistence in db.

Safety: never enumerates other servers; only considers libraries rooted under
PLEX_SCAN_MOUNT_ROOT; feature is gated by PLEX_SCAN_ENABLED.
"""

import datetime
import time
from urllib.parse import urlencode

import requests
from starlette.config import Config

import rq

from . import db, log as _log, scan_paths as sp
from .utilities import redis_connection

logger = _log.get("scan")
kv = _log.kv
config = Config()

SCAN_ENABLED = config("PLEX_SCAN_ENABLED", cast=bool, default=False)
SCAN_URL = config("PLEX_SCAN_URL", cast=str, default="").rstrip("/")
# Fallback used ONLY if PLEX_SCAN_URL is unreachable from inside the container. On
# Windows/WSL2 the host's LAN IP is often not routable from the container, but the
# Docker host alias is -- so a Plex on the host is reachable at host.docker.internal.
# We never guess a random server: the fallback is a single well-known host alias, and
# we only switch to it after confirming the CONFIGURED url failed and the fallback's
# /identity actually responds. Set to "" to disable the fallback entirely.
SCAN_URL_FALLBACK = config("PLEX_SCAN_URL_FALLBACK", cast=str, default="http://host.docker.internal:32400").rstrip("/")
SCAN_TOKEN = config("PLEX_TOKEN", cast=str, default="")
SCAN_MOUNT_ROOT = config("PLEX_SCAN_MOUNT_ROOT", cast=str, default="R:")
SCAN_INTERVAL_MIN = config("PLEX_SCAN_INTERVAL_MIN", cast=int, default=5)
SCAN_WINDOW_START = config("PLEX_SCAN_WINDOW_START", cast=str, default="07:30")
SCAN_WINDOW_END = config("PLEX_SCAN_WINDOW_END", cast=str, default="11:30")
SCAN_TZ = config("PLEX_SCAN_TZ", cast=str, default="Europe/London")
SCAN_MAX_WAIT_MIN = config("PLEX_SCAN_MAX_WAIT_MIN", cast=int, default=30)
SCAN_COOLDOWN_MIN = config("PLEX_SCAN_COOLDOWN_MIN", cast=int, default=360)

CONTAINER_SIZE = config("PLEX_CONTAINER_SIZE", cast=int, default=400)
_TYPE_NUM = {"movies": 1, "shows": 4}  # section /all item type: movie=1, episode=4

rq_queue = rq.Queue(name="default", connection=redis_connection)
rq_retries = rq.Retry(max=3, interval=[10, 30, 120])
# The worker blocks up to SCAN_MAX_WAIT_MIN polling /activities for one scan to finish.
# rq's default job timeout is only 180s, which would kill (and retry) the worker mid-poll
# on a large season scan -> duplicate scan requests. Give the worker job a ceiling above
# its max blocking wait so rq lets it run to completion.
WORKER_JOB_TIMEOUT = SCAN_MAX_WAIT_MIN * 60 + 120
_session = requests.Session()

# The base URL actually used for requests. Defaults to the configured URL and is
# (re)resolved at the start of each task -- rq forks a process per job, so this must be
# decided per-run, not once at import.
_active_url = SCAN_URL


def _reachable(base_url: str) -> bool:
    """True if base_url's Plex answers /identity (auth not required for identity)."""
    if not base_url:
        return False
    try:
        return _session.get(f"{base_url}/identity", timeout=5).status_code == 200
    except requests.RequestException:
        return False


def _resolve_scan_url() -> str:
    """Pick the base URL to talk to THIS run. Prefer the configured PLEX_SCAN_URL; only
    if it is unreachable AND the fallback host alias responds, switch to the fallback
    (handles WSL2 where the host LAN IP isn't routable from the container). Logs the
    choice. Returns "" if neither is reachable (callers then no-op that run)."""
    global _active_url
    if _reachable(SCAN_URL):
        _active_url = SCAN_URL
    elif SCAN_URL_FALLBACK and _reachable(SCAN_URL_FALLBACK):
        _active_url = SCAN_URL_FALLBACK
        logger.warning("scan_url_fallback " + kv(configured=SCAN_URL, using=SCAN_URL_FALLBACK))
    else:
        _active_url = ""
        logger.warning("scan_url_unreachable " + kv(configured=SCAN_URL, fallback=SCAN_URL_FALLBACK))
    return _active_url


def _get(path: str, **params) -> dict:
    params["X-Plex-Token"] = SCAN_TOKEN
    try:
        resp = _session.get(
            f"{_active_url}{path}?{urlencode(params)}", headers={"Accept": "application/json"}, timeout=20
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        # log the endpoint (token stripped) so a failure against our own Plex is
        # diagnosable from the worker logs, then re-raise for rq to record/retry.
        status = getattr(getattr(e, "response", None), "status_code", "none")
        logger.warning("plex_get_failed " + kv(path=path, error=type(e).__name__, status=status))
        raise
    return resp.json().get("MediaContainer", {})


def get_scan_sections() -> list[dict]:
    """Sections on the target server whose Location root is under SCAN_MOUNT_ROOT."""
    mc = _get("/library/sections")
    out = []
    type_map = {"movie": "movies", "show": "shows"}
    for d in mc.get("Directory", []):
        mtype = type_map.get(d.get("type"))
        if not mtype:
            continue
        for loc in d.get("Location", []):
            root = loc.get("path", "")
            if root.startswith(SCAN_MOUNT_ROOT):
                out.append({"id": str(d.get("key")), "type": mtype, "root": root})
                break
    return out


def iter_section_part_files(section_id: str, media_type: str) -> set[str]:
    """Every Part.file Plex knows for a section (paged)."""
    files: set[str] = set()
    start = 0
    tnum = _TYPE_NUM[media_type]
    while True:
        mc = _get(
            f"/library/sections/{section_id}/all",
            type=tnum,
            **{"X-Plex-Container-Start": start, "X-Plex-Container-Size": CONTAINER_SIZE},
        )
        items = mc.get("Metadata", [])
        for it in items:
            for m in it.get("Media", []):
                for p in m.get("Part", []):
                    if p.get("file"):
                        files.add(p["file"])
        total = mc.get("totalSize", mc.get("size", 0))
        start += CONTAINER_SIZE
        if start >= total or not items:
            break
    return files


def request_scan(section_id: str, windows_path: str) -> int:
    """Fire a partial scan of one folder; return HTTP status."""
    resp = _session.get(
        f"{_active_url}/library/sections/{section_id}/refresh",
        params={"path": windows_path, "X-Plex-Token": SCAN_TOKEN},
        timeout=20,
    )
    return resp.status_code


def scan_in_progress() -> bool:
    """True if Plex currently has a library-scan activity running."""
    mc = _get("/activities")
    return any(a.get("type") == "library.update.section" for a in mc.get("Activity", []))


def _section_scope_prefix(section_root: str) -> str:
    """The section's own (possibly NARROW) root in catalog form, used as an in-scope
    prefix test. If TV is rooted at r"R:\\shows\\<guid>\\batch01", this returns
    "shows/<guid>/batch01" -- so only folders under it are scannable; batch02/03 and
    other guids come back as 'blocked'. If rooted at r"R:\\shows", returns "shows"
    (everything in scope)."""
    cat = sp.plex_part_to_catalog(section_root + "\\_", SCAN_MOUNT_ROOT)  # append/strip a stub
    return cat.rsplit("/", 1)[0] if cat else ""


def reconcile_scan_state() -> None:
    """Compare the catalog against what the target Plex already has; write scan_state.
    Read-only against Plex (existing metadata only) -> zero friend impact. Re-enqueues
    the scan worker if the feature is enabled.

    All path conversion is done against SCAN_MOUNT_ROOT (the drive level). Each
    section's OWN root -- which may be narrower than the mount (e.g. a single batch
    folder) -- is used only to decide scope: folders outside it become 'blocked'."""
    if not SCAN_ENABLED:
        return
    if not _resolve_scan_url():
        return  # neither configured URL nor fallback reachable -> skip this run
    sections = get_scan_sections()
    # 1. gather Plex-present files (catalog form) + the scope prefix per media_type
    plex_by_type: dict[str, set[str]] = {}
    scope_by_type: dict[str, str] = {}
    for s in sections:
        present = set()
        for pf in iter_section_part_files(s["id"], s["type"]):
            cat = sp.plex_part_to_catalog(pf, SCAN_MOUNT_ROOT)
            if cat:
                present.add(cat)
        plex_by_type[s["type"]] = present
        scope_by_type[s["type"]] = _section_scope_prefix(s["root"])

    # 2. for each media_type + node, build catalog serving paths and diff
    for media_type, plex_files in plex_by_type.items():
        scope_prefix = scope_by_type[media_type]

        def in_scope(folder: str, _p=scope_prefix) -> bool:
            # in scope iff the folder sits under the section's (possibly narrow) root
            return folder == _p or folder.startswith(_p + "/")

        for node in _catalog_nodes(media_type):
            batch_of = _batch_map_for(node, media_type)
            catalog_files = db.catalog_serving_paths(node, media_type, batch_of)
            statuses = sp.diff_folders(catalog_files, plex_files, in_scope)
            db.replace_scan_state(node, media_type, statuses)

    counts = db.count_scan_state()
    logger.info("reconcile " + kv(**counts))
    rq_queue.enqueue(
        "tasks.plex_scan_worker", job_id="plex_scan_worker", retry=rq_retries, job_timeout=WORKER_JOB_TIMEOUT
    )


def _catalog_nodes(media_type: str) -> list[str]:
    with db._conn() as c:
        return [
            r["server_node"]
            for r in c.execute("SELECT DISTINCT server_node FROM media WHERE media_type = ?", (media_type,)).fetchall()
        ]


def _batch_map_for(node: str, media_type: str) -> dict | None:
    """Return the persisted show->batch map if shows are batched for this node, else None."""
    if media_type != "shows":
        return None
    with db._conn() as c:
        rows = c.execute("SELECT show_name, batch FROM show_batch WHERE server_node = ?", (node,)).fetchall()
    return {r["show_name"]: r["batch"] for r in rows} if rows else None


def plex_scan_worker() -> None:
    """Scan at most ONE pending folder, then re-enqueue self in SCAN_INTERVAL_MIN.
    No-op outside the daily window or when disabled. Waits for the scan activity to
    clear (up to SCAN_MAX_WAIT_MIN) before returning, so scans never overlap."""
    if not SCAN_ENABLED:
        return

    def _reenqueue():
        rq_queue.enqueue_in(
            datetime.timedelta(minutes=SCAN_INTERVAL_MIN),
            "tasks.plex_scan_worker",
            job_id="plex_scan_worker",
            retry=rq_retries,
            job_timeout=WORKER_JOB_TIMEOUT,
        )

    now_local = sp.now_in_tz(SCAN_TZ)
    if not sp.in_window(now_local, SCAN_WINDOW_START, SCAN_WINDOW_END):
        logger.debug("skip " + kv(reason="out_of_window", local=now_local.strftime("%H:%M %Z")))
        _reenqueue()
        return

    if not _resolve_scan_url():  # Plex not reachable this run -> try again next interval
        _reenqueue()
        return

    section_ids = {s["type"]: s["id"] for s in get_scan_sections()}
    now = int(time.time())
    cooldown = SCAN_COOLDOWN_MIN * 60

    target = None
    for row in db.get_pending_scans():
        if row["media_type"] not in section_ids:
            continue
        if row["requested_at"] and (now - row["requested_at"]) < cooldown:
            continue
        target = row
        break

    if target is None:
        logger.debug("skip " + kv(reason="nothing_pending"))
        _reenqueue()
        return

    section_id = section_ids[target["media_type"]]
    win_path = sp.catalog_to_windows(target["scan_path"], SCAN_MOUNT_ROOT)
    status = request_scan(section_id, win_path)
    db.mark_scan_requested(target["server_node"], target["media_type"], target["scan_path"], now)
    logger.info("scan " + kv(path=target["scan_path"], http=status))

    # wait for the scan activity to clear (bounded)
    deadline = time.monotonic() + SCAN_MAX_WAIT_MIN * 60
    time.sleep(3)  # give Plex a moment to register the activity
    while time.monotonic() < deadline:
        if not scan_in_progress():
            db.mark_scan_done(target["server_node"], target["media_type"], target["scan_path"], int(time.time()))
            logger.info("scan_done " + kv(path=target["scan_path"]))
            break
        time.sleep(5)
    else:
        # scan never confirmed complete within the cap -> revert to 'pending' so the
        # next reconcile re-checks it against Plex instead of leaving it stuck in
        # 'requested' forever. requested_at stays set, so the cooldown still throttles.
        db.mark_scan_pending(target["server_node"], target["media_type"], target["scan_path"])
        logger.warning("scan_timeout " + kv(path=target["scan_path"], cap_min=SCAN_MAX_WAIT_MIN))

    _reenqueue()
