"""SQLite persistence layer: the durable source of truth for the media catalog.

Design
------
The catalog is stored here rather than only in Redis so that it survives restarts
and is never lost to key expiry:

  * The crawler UPSERTs discovered media into SQLite (no TTL -- rows live until the
    content actually changes upstream).
  * Redis is rebuilt FROM this table via an atomic swap and only ever serves a
    complete snapshot, so readers never observe a half-built catalog.
  * On restart the Redis listing is repopulated from SQLite instead of re-crawling
    the source servers.

Stdlib sqlite3 only -- no extra dependency. The DB lives at /pr/reshare.db, which
persists across restarts via the existing ./:/pr bind mount.
"""

import os
import sqlite3
import time
from contextlib import contextmanager

DB_PATH = os.getenv("RESHARE_DB_PATH", "/pr/reshare.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS media (
    server_node TEXT NOT NULL,   -- plex clientIdentifier (the R:\\ GUID)
    media_type  TEXT NOT NULL,   -- 'movies' | 'shows'
    rating_key  TEXT NOT NULL,   -- plex stable id (movie, or episode)
    show_key    TEXT,            -- for episodes: parent show ratingKey (NULL for movies)
    file_path   TEXT NOT NULL,   -- listing path -> becomes the R: entry
    plex_key    TEXT NOT NULL,   -- /library/parts/... -> what nginx proxies to
    size        INTEGER,         -- file size in bytes; lets nginx answer HEAD locally
    duration    INTEGER,         -- runtime in ms (from Media.duration)
    container   TEXT,            -- e.g. 'mkv', 'mp4' (from Part/Media.container)
    resolution  TEXT,            -- e.g. '1080', '4k' (from Media.videoResolution)
    title       TEXT,
    updated_at  INTEGER,
    added_at    INTEGER,
    PRIMARY KEY (server_node, media_type, rating_key)
);

CREATE INDEX IF NOT EXISTS idx_media_node_type
    ON media (server_node, media_type);

-- for "replace all episodes of one show" (per-show atomic replace)
CREATE INDEX IF NOT EXISTS idx_media_show
    ON media (server_node, show_key);

-- Per-show change-detection state for the incremental crawl.
CREATE TABLE IF NOT EXISTS show_state (
    server_node TEXT NOT NULL,
    show_key    TEXT NOT NULL,   -- show ratingKey
    leaf_count  INTEGER,         -- last-seen episode count
    updated_at  INTEGER,         -- last-seen show updatedAt
    PRIMARY KEY (server_node, show_key)
);

-- Stable show->batch assignment (only used when SHOWS_PER_BATCH > 0). Lets a very
-- large TV library be split into fixed-size batchNN/ folders in the listing. The
-- assignment is PERSISTED so it is stable: an existing show never changes batch, so
-- Plex rescans a show at most once. Keyed by the show's folder name (the first path
-- segment), which is exactly how the flat listing already groups a show's episodes.
CREATE TABLE IF NOT EXISTS show_batch (
    server_node TEXT NOT NULL,
    show_name   TEXT NOT NULL,   -- show folder name (file_path's first segment)
    batch       INTEGER NOT NULL,-- 1-based batch number -> batchNN/ folder
    PRIMARY KEY (server_node, show_name)
);

-- Bookkeeping so restarts can decide "rebuild from DB" vs "re-crawl".
CREATE TABLE IF NOT EXISTS crawl_meta (
    server_node     TEXT NOT NULL,
    media_type      TEXT NOT NULL,
    last_full_crawl INTEGER,     -- unix ts of last successful FULL crawl
    last_crawl      INTEGER,     -- unix ts of last successful crawl (any)
    last_status     TEXT,        -- 'ok' | 'partial' | 'failed'
    PRIMARY KEY (server_node, media_type)
);

-- Per-folder Plex scan tracking (targeted-scan feature). A "folder" is a movie
-- folder or a season folder as Plex sees it on the mount. Populated by reconcile;
-- drained by the scan worker one folder per interval.
CREATE TABLE IF NOT EXISTS scan_state (
    server_node  TEXT NOT NULL,   -- friend clientIdentifier (the mount GUID)
    media_type   TEXT NOT NULL,   -- 'movies' | 'shows'
    scan_path    TEXT NOT NULL,   -- mount-relative FOLDER, e.g. movies/<guid>/Title
    status       TEXT NOT NULL,   -- 'pending' | 'requested' | 'scanned' | 'blocked'
    requested_at INTEGER,         -- unix secs; last scan request
    scanned_at   INTEGER,         -- unix secs; confirmed present in Plex
    PRIMARY KEY (server_node, media_type, scan_path)
);

CREATE INDEX IF NOT EXISTS idx_scan_status ON scan_state (status);
"""


@contextmanager
def _conn():
    """One connection per call. sqlite3 connections are not safe to share across
    the separate rq-worker and starlette processes, so we open/close per use.
    WAL mode lets the worker write while the app reads without blocking."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    """Create tables if missing. Safe to call repeatedly (idempotent)."""
    with _conn() as c:
        c.executescript(SCHEMA)


# --------------------------------------------------------------------------- #
# Media upsert / read
# --------------------------------------------------------------------------- #
def _norm_rows(rows: list[dict]) -> list[dict]:
    """Ensure optional keys exist so executemany's named params never KeyError."""
    for row in rows:
        row.setdefault("show_key", None)
        row.setdefault("size", None)
        row.setdefault("duration", None)
        row.setdefault("container", None)
        row.setdefault("resolution", None)
    return rows


_INSERT_MEDIA = """
    INSERT INTO media (server_node, media_type, rating_key, show_key, file_path,
                       plex_key, size, duration, container, resolution, title,
                       updated_at, added_at)
    VALUES (:server_node, :media_type, :rating_key, :show_key, :file_path,
            :plex_key, :size, :duration, :container, :resolution, :title,
            :updated_at, :added_at)
    ON CONFLICT(server_node, media_type, rating_key) DO UPDATE SET
        show_key=excluded.show_key,
        file_path=excluded.file_path,
        plex_key=excluded.plex_key,
        size=excluded.size,
        duration=excluded.duration,
        container=excluded.container,
        resolution=excluded.resolution,
        title=excluded.title,
        updated_at=excluded.updated_at,
        added_at=excluded.added_at
"""


def upsert_media_batch(rows: list[dict]) -> None:
    """Insert-or-update a batch of media rows. Keys: server_node, media_type,
    rating_key, show_key (None for movies), file_path, plex_key, size, title,
    updated_at, added_at. Accumulates with NO TTL."""
    if not rows:
        return
    with _conn() as c:
        c.executemany(_INSERT_MEDIA, _norm_rows(rows))


def replace_show_episodes(server_node: str, show_key: str, rows: list[dict]) -> None:
    """Atomically replace ALL episodes of one show: delete the show's existing
    episodes, then insert the freshly-fetched set -- in a single transaction. This
    is how allLeaves keeps a changed show consistent (stale episodes removed, new
    ones added) without touching other shows. Deleting an episode upstream shrinks
    leafCount -> triggers a re-walk -> lands here -> the stale row disappears."""
    with _conn() as c:
        c.execute(
            "DELETE FROM media WHERE server_node = ? AND media_type = 'shows' AND show_key = ?",
            (server_node, show_key),
        )
        if rows:
            c.executemany(_INSERT_MEDIA, _norm_rows(rows))


def delete_shows(server_node: str, show_keys: list[str]) -> int:
    """Remove whole shows (their episodes + show_state) that vanished upstream.
    Returns episodes deleted."""
    if not show_keys:
        return 0
    with _conn() as c:
        n = 0
        for sk in show_keys:
            cur = c.execute(
                "DELETE FROM media WHERE server_node = ? AND media_type = 'shows' AND show_key = ?",
                (server_node, sk),
            )
            n += cur.rowcount
            c.execute(
                "DELETE FROM show_state WHERE server_node = ? AND show_key = ?",
                (server_node, sk),
            )
        return n


def get_media(server_node: str, media_type: str) -> list[sqlite3.Row]:
    """All media rows for one server+type -- used to rebuild the Redis listing.
    `mtime` is the file's modification time (Plex updatedAt, falling back to addedAt)
    so nginx can report a real Last-Modified instead of the zero epoch."""
    with _conn() as c:
        return c.execute(
            "SELECT file_path, plex_key, size, COALESCE(updated_at, added_at) AS mtime "
            "FROM media WHERE server_node = ? AND media_type = ?",
            (server_node, media_type),
        ).fetchall()


def catalog_serving_paths(server_node: str, media_type: str, batch_of: dict | None) -> set[str]:
    """Mount-relative catalog FILE paths for a node+type, matching exactly what the
    Redis serving listing uses (including the optional batchNN/ prefix for shows).
    `batch_of` maps show_name -> batch number when batching is on, else None."""
    out: set[str] = set()
    with _conn() as c:
        rows = c.execute(
            "SELECT file_path FROM media WHERE server_node = ? AND media_type = ?",
            (server_node, media_type),
        ).fetchall()
    for r in rows:
        fp = r["file_path"]
        if batch_of:
            show = fp.split("/", 1)[0]
            b = batch_of.get(show)
            if b is not None:
                fp = f"batch{b:02d}/{fp}"
        out.add(f"{media_type}/{server_node}/{fp}")
    return out


def get_all_media() -> list[sqlite3.Row]:
    """Every media row -- used to repopulate all of Redis on a fresh restart."""
    with _conn() as c:
        return c.execute("SELECT server_node, media_type, file_path, plex_key, size FROM media").fetchall()


def count_media(server_node: str, media_type: str) -> int:
    with _conn() as c:
        return c.execute(
            "SELECT COUNT(*) FROM media WHERE server_node = ? AND media_type = ?",
            (server_node, media_type),
        ).fetchone()[0]


def get_rating_keys(server_node: str, media_type: str) -> set[str]:
    """Set of rating_keys we currently have stored -- for the Stage-3 deletion diff."""
    with _conn() as c:
        return {
            r[0]
            for r in c.execute(
                "SELECT rating_key FROM media WHERE server_node = ? AND media_type = ?",
                (server_node, media_type),
            ).fetchall()
        }


def delete_media_keys(server_node: str, media_type: str, rating_keys: list[str]) -> None:
    """Remove specific items (deletion diff, or lazy-delete reconciliation)."""
    if not rating_keys:
        return
    with _conn() as c:
        c.executemany(
            "DELETE FROM media WHERE server_node = ? AND media_type = ? AND rating_key = ?",
            [(server_node, media_type, rk) for rk in rating_keys],
        )


def lazy_delete_path(server_node: str, media_type: str, file_path: str) -> bool:
    """Lazy-delete ONE item (a single movie or a single episode) whose playback
    returned a definitive 404. Deletes only that row -- NEVER the whole show.

    If it was an episode, also clear the parent show's change-detection state so the
    next crawl re-verifies the show via allLeaves: this self-heals either way -- if
    the 404 was transient the episode reappears, if it was truly deleted it stays
    gone. Returns True if a row was removed. Movies just drop the single row.
    """
    with _conn() as c:
        row = c.execute(
            "SELECT show_key FROM media WHERE server_node=? AND media_type=? AND file_path=?",
            (server_node, media_type, file_path),
        ).fetchone()
        if row is None:
            return False
        c.execute(
            "DELETE FROM media WHERE server_node=? AND media_type=? AND file_path=?",
            (server_node, media_type, file_path),
        )
        if media_type == "shows" and row["show_key"]:
            c.execute(
                "DELETE FROM show_state WHERE server_node=? AND show_key=?",
                (server_node, row["show_key"]),
            )
        return True


# --------------------------------------------------------------------------- #
# show_state (per-show change-detection)
# --------------------------------------------------------------------------- #
def get_show_state(server_node: str) -> dict[str, dict]:
    """{show_key: {'leaf_count': int, 'updated_at': int}} for change detection."""
    with _conn() as c:
        return {
            r["show_key"]: {"leaf_count": r["leaf_count"], "updated_at": r["updated_at"]}
            for r in c.execute(
                "SELECT show_key, leaf_count, updated_at FROM show_state WHERE server_node = ?",
                (server_node,),
            ).fetchall()
        }


def upsert_show_state(server_node: str, show_key: str, leaf_count: int, updated_at: int) -> None:
    with _conn() as c:
        c.execute(
            """
            INSERT INTO show_state (server_node, show_key, leaf_count, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(server_node, show_key) DO UPDATE SET
                leaf_count=excluded.leaf_count, updated_at=excluded.updated_at
            """,
            (server_node, show_key, leaf_count, updated_at),
        )


# --------------------------------------------------------------------------- #
# show_batch (stable show -> batchNN assignment for large TV libraries)
# --------------------------------------------------------------------------- #
def sync_show_batches(server_node: str, show_names: list[str], batch_size: int) -> dict[str, int]:
    """Reconcile the stable show->batch map for one server and return {show_name: batch}.

    Guarantees an existing show NEVER changes batch (so Plex rescans a show at most
    once), while keeping batches compact:

      1. drop assignments for shows that vanished upstream  -> frees their slot
      2. keep every surviving show's batch exactly as-is     -> never move
      3. place each NEW show into the LOWEST-numbered batch that has a free slot
         (reusing holes left by deletions); open a new batch only when all are full

    Deterministic: new shows are placed in sorted order, so concurrent worker jobs
    that call this with the same set converge to the same assignment. Runs in a single
    transaction. `batch_size` must be > 0 (caller decides whether batching is on)."""
    present = set(show_names)
    with _conn() as c:
        existing = {
            row["show_name"]: row["batch"]
            for row in c.execute(
                "SELECT show_name, batch FROM show_batch WHERE server_node = ?",
                (server_node,),
            ).fetchall()
        }

        # 1. drop shows no longer present (free their slots)
        gone = [name for name in existing if name not in present]
        if gone:
            c.executemany(
                "DELETE FROM show_batch WHERE server_node = ? AND show_name = ?",
                [(server_node, name) for name in gone],
            )
            for name in gone:
                del existing[name]

        # 2. surviving shows keep their batch; count occupancy per batch
        counts: dict[int, int] = {}
        for b in existing.values():
            counts[b] = counts.get(b, 0) + 1

        # 3. assign new shows to the lowest batch with a free slot, else a new batch
        new_names = sorted(name for name in present if name not in existing)
        max_batch = max(counts) if counts else 0
        additions = []
        for name in new_names:
            target = None
            for b in range(1, max_batch + 1):
                if counts.get(b, 0) < batch_size:
                    target = b
                    break
            if target is None:  # every existing batch full -> open the next one
                max_batch += 1
                target = max_batch
            counts[target] = counts.get(target, 0) + 1
            existing[name] = target
            additions.append((server_node, name, target))
        if additions:
            c.executemany(
                "INSERT INTO show_batch (server_node, show_name, batch) VALUES (?, ?, ?)",
                additions,
            )

        return dict(existing)


def clear_show_batches(server_node: str) -> None:
    """Forget all batch assignments for a server (used when batching is turned off, so
    a later re-enable starts fresh instead of reusing stale batch numbers)."""
    with _conn() as c:
        c.execute("DELETE FROM show_batch WHERE server_node = ?", (server_node,))


# --------------------------------------------------------------------------- #
# crawl_meta (restart / staleness bookkeeping)
# --------------------------------------------------------------------------- #
def record_crawl(server_node: str, media_type: str, status: str, is_full: bool, ts: int | None = None) -> None:
    """Stamp a crawl outcome. `ts` lets the caller pass a fixed time (tests);
    defaults to now."""
    ts = int(time.time()) if ts is None else ts
    with _conn() as c:
        existing = c.execute(
            "SELECT last_full_crawl FROM crawl_meta WHERE server_node = ? AND media_type = ?",
            (server_node, media_type),
        ).fetchone()
        last_full = ts if is_full else (existing["last_full_crawl"] if existing else None)
        c.execute(
            """
            INSERT INTO crawl_meta (server_node, media_type, last_full_crawl, last_crawl, last_status)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(server_node, media_type) DO UPDATE SET
                last_full_crawl=excluded.last_full_crawl,
                last_crawl=excluded.last_crawl,
                last_status=excluded.last_status
            """,
            (server_node, media_type, last_full, ts, status),
        )


def has_any_media() -> bool:
    with _conn() as c:
        return c.execute("SELECT 1 FROM media LIMIT 1").fetchone() is not None


def last_full_crawl_ts(server_node: str, media_type: str) -> int | None:
    """Timestamp of the last FULL crawl for a node/type -- used to decide when the
    weekly from-scratch reconciliation rebuild is due. None if never."""
    with _conn() as c:
        row = c.execute(
            "SELECT last_full_crawl FROM crawl_meta WHERE server_node = ? AND media_type = ?",
            (server_node, media_type),
        ).fetchone()
        return row[0] if row and row[0] is not None else None


def node_last_crawl_ts(server_node: str) -> int | None:
    """Oldest successful last_crawl across this node's media types (SQLite-backed, so
    it survives restarts). Used to gate the 5-12h light refresh: a restart must NOT
    reset this (unlike the old Redis throttle key that got wiped every restart).
    None if the node has never been crawled -> caller should crawl."""
    with _conn() as c:
        row = c.execute(
            "SELECT MIN(last_crawl) FROM crawl_meta WHERE server_node = ? AND last_status IN ('ok', 'partial')",
            (server_node,),
        ).fetchone()
        return row[0] if row and row[0] is not None else None


# --------------------------------------------------------------------------- #
# scan_state (targeted Plex scan tracking)
# --------------------------------------------------------------------------- #
def replace_scan_state(server_node: str, media_type: str, folders: dict[str, str]) -> None:
    """Reconcile scan_state for one (node, media_type) to `folders` = {scan_path: status}.

    Upserts each folder's status; deletes rows whose scan_path is no longer present.
    Preserves requested_at across reconciles (so a re-run does not reset the cooldown);
    stamps scanned_at when a row's status becomes 'scanned'."""
    with _conn() as c:
        existing = {
            r["scan_path"]: r
            for r in c.execute(
                "SELECT scan_path, status, requested_at, scanned_at FROM scan_state "
                "WHERE server_node = ? AND media_type = ?",
                (server_node, media_type),
            ).fetchall()
        }
        keep = set(folders)
        stale = [p for p in existing if p not in keep]
        if stale:
            c.executemany(
                "DELETE FROM scan_state WHERE server_node=? AND media_type=? AND scan_path=?",
                [(server_node, media_type, p) for p in stale],
            )
        now = int(time.time())
        for path, status in folders.items():
            prior = existing.get(path)
            req_at = prior["requested_at"] if prior else None
            scanned_at = prior["scanned_at"] if prior else None
            if status == "scanned" and (prior is None or prior["status"] != "scanned"):
                scanned_at = now
            c.execute(
                """
                INSERT INTO scan_state (server_node, media_type, scan_path, status, requested_at, scanned_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(server_node, media_type, scan_path) DO UPDATE SET
                    status=excluded.status,
                    requested_at=excluded.requested_at,
                    scanned_at=excluded.scanned_at
                """,
                (server_node, media_type, path, status, req_at, scanned_at),
            )


def get_pending_scans() -> list[sqlite3.Row]:
    """Folders awaiting a scan, oldest-request-first (never-requested first)."""
    with _conn() as c:
        return c.execute(
            "SELECT server_node, media_type, scan_path, status, requested_at, scanned_at "
            "FROM scan_state WHERE status = 'pending' "
            "ORDER BY COALESCE(requested_at, 0) ASC, scan_path ASC"
        ).fetchall()


def mark_scan_requested(server_node: str, media_type: str, scan_path: str, ts: int) -> None:
    """Mark a scan folder as 'requested', stamping the request timestamp."""
    with _conn() as c:
        c.execute(
            "UPDATE scan_state SET status='requested', requested_at=? "
            "WHERE server_node=? AND media_type=? AND scan_path=?",
            (ts, server_node, media_type, scan_path),
        )


def mark_scan_done(server_node: str, media_type: str, scan_path: str, ts: int) -> None:
    """Mark a scan folder as 'scanned', stamping the completion timestamp."""
    with _conn() as c:
        c.execute(
            "UPDATE scan_state SET status='scanned', scanned_at=? "
            "WHERE server_node=? AND media_type=? AND scan_path=?",
            (ts, server_node, media_type, scan_path),
        )


def mark_scan_pending(server_node: str, media_type: str, scan_path: str) -> None:
    """Revert a scan folder to 'pending' (e.g. after a scan timed out without a
    confirmed completion). Keeps requested_at intact so the cooldown still throttles
    re-requests; the next reconcile re-checks it against Plex and self-heals: if the
    folder is actually present it flips to 'scanned', otherwise it stays 'pending'."""
    with _conn() as c:
        c.execute(
            "UPDATE scan_state SET status='pending' WHERE server_node=? AND media_type=? AND scan_path=?",
            (server_node, media_type, scan_path),
        )


def count_scan_state() -> dict[str, int]:
    """Count scan_state rows by status across all (node, media_type) pairs."""
    with _conn() as c:
        return {
            r["status"]: r["n"]
            for r in c.execute("SELECT status, COUNT(*) AS n FROM scan_state GROUP BY status").fetchall()
        }
