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

-- Bookkeeping so restarts can decide "rebuild from DB" vs "re-crawl".
CREATE TABLE IF NOT EXISTS crawl_meta (
    server_node     TEXT NOT NULL,
    media_type      TEXT NOT NULL,
    last_full_crawl INTEGER,     -- unix ts of last successful FULL crawl
    last_crawl      INTEGER,     -- unix ts of last successful crawl (any)
    last_status     TEXT,        -- 'ok' | 'partial' | 'failed'
    PRIMARY KEY (server_node, media_type)
);
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
    """All media rows for one server+type -- used to rebuild the Redis listing."""
    with _conn() as c:
        return c.execute(
            "SELECT file_path, plex_key, size FROM media "
            "WHERE server_node = ? AND media_type = ?",
            (server_node, media_type),
        ).fetchall()


def get_all_media() -> list[sqlite3.Row]:
    """Every media row -- used to repopulate all of Redis on a fresh restart."""
    with _conn() as c:
        return c.execute(
            "SELECT server_node, media_type, file_path, plex_key, size FROM media"
        ).fetchall()


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
            "SELECT MIN(last_crawl) FROM crawl_meta WHERE server_node = ? "
            "AND last_status IN ('ok', 'partial')",
            (server_node,),
        ).fetchone()
        return row[0] if row and row[0] is not None else None
