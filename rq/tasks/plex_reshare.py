import datetime
import json
import random
import re
import socket
import string
import time
from urllib.parse import urlencode, urlparse

import pickledb
import redis
import requests
from starlette.config import Config

import rq

from . import db, log as _log
from .utilities import redis_connection

logger = _log.get("crawl")
kv = _log.kv

config = Config()
PLEX_TOKEN = config("PLEX_TOKEN", cast=str, default="")
DEVELOPMENT = config("DEVELOPMENT", cast=bool, default=False)
IGNORE_PLAYLIST = config("IGNORE_PLAYLIST", cast=str, default="")
REDIS_REFRESH_TTL = 3 * 60 * 60  # server-list refresh cadence (NOT a listing TTL)
IGNORE_EXTENSIONS = config("IGNORE_EXTENSIONS", cast=str, default="").split(",") + [None]
IGNORE_RESOLUTIONS = config("IGNORE_RESOLUTIONS", cast=str, default="").split(",") + [None]
IGNORE_MOVIE_TEMPLATES = [i for i in config("IGNORE_MOVIE_TEMPLATES", cast=str, default="").split("|") if i]
IGNORE_EPISODE_TEMPLATES = [i for i in config("IGNORE_EPISODE_TEMPLATES", cast=str, default="").split("|") if i]
MOVIE_MIN_SIZE = config("MOVIE_MIN_SIZE", cast=int, default=512)
EPISODE_MIN_SIZE = config("EPISODE_MIN_SIZE", cast=int, default=64)
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"
    " (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
}

# Items fetched per page. Upstream hard-coded 100; a larger page means far fewer
# requests to the source server. Values well above 100 are accepted by Plex.
CONTAINER_SIZE = config("PLEX_CONTAINER_SIZE", cast=int, default=400)

# Refresh cadence. A node is re-crawled on a random gap in this range, so all source
# servers aren't hit on the same schedule. Refreshes are incremental (only changed
# shows are re-fetched) so most are cheap. A periodic from-scratch rebuild reconciles
# any updatedAt/leafCount drift.
REFRESH_HOURS_MIN = config("REFRESH_HOURS_MIN", cast=int, default=5)
REFRESH_HOURS_MAX = config("REFRESH_HOURS_MAX", cast=int, default=12)
# Weekly-ish full rebuild, but RANDOMIZED across a range so the heavy crawl doesn't
# land on an exact clockwork interval (an exact 7.000-day cadence looks obviously
# automated in a friend's server logs). The threshold is seeded from last_full_crawl
# so it stays STABLE across the many intermediate 5-12h checks, then re-rolls once a
# full crawl actually happens.
FULL_CRAWL_DAYS_MIN = config("FULL_CRAWL_DAYS_MIN", cast=int, default=6)
FULL_CRAWL_DAYS_MAX = config("FULL_CRAWL_DAYS_MAX", cast=int, default=9)

# Optionally split a large TV library into fixed-size batchNN/ folders in the listing
# so no single shows/<guid>/ folder holds hundreds of shows. <= 0 disables batching
# (flat shows/<guid>/<show>/...). The per-show batch assignment is PERSISTED and
# stable, so an existing show never changes batch -> Plex rescans a show at most once.
SHOWS_PER_BATCH = config("SHOWS_PER_BATCH", cast=int, default=-1)

# One keep-alive Session reused across requests to a friend's server -> avoids a new
# TLS handshake per call (gentler + faster). rq workers are separate processes, so a
# module-level Session is safe here.
session = requests.Session()
session.headers.update(HEADERS)

r = redis.Redis(
    host=config("REDIS_HOST", default="localhost"),
    port=config("REDIS_PORT", cast=int, default=6379),
    db=11,
    decode_responses=True,
)
rq_queue = rq.Queue(name="default", connection=redis_connection)
rq_retries = rq.Retry(max=3, interval=[10, 30, 120])


reqlog = _log.get("request")


def _plex_get(uri: str, path: str, token: str, timeout: int = 20, **params) -> dict:
    """GET a Plex endpoint and return its MediaContainer dict (or {} if absent).
    Centralizes token injection, the Session, timeout, and defensive parsing.

    This is the SINGLE chokepoint for every request to a friend's server, so we log
    each one here: the path (token stripped), HTTP status, byte size, and elapsed ms.
    That gives an exact request count + timing per crawl and surfaces errors with
    context -- exactly what's needed to diagnose from shared logs."""
    params["X-Plex-Token"] = token
    started = time.monotonic()
    try:
        resp = session.get(f"https://{uri}{path}?{urlencode(params)}", timeout=timeout)
        elapsed_ms = int((time.monotonic() - started) * 1000)
        resp.raise_for_status()
        reqlog.info(
            kv(
                node=uri.split(".")[1] if "." in uri else uri,
                path=path,
                status=resp.status_code,
                bytes=len(resp.content),
                ms=elapsed_ms,
            )
        )
        return resp.json().get("MediaContainer", {})
    except requests.exceptions.Timeout:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        reqlog.warning(kv(path=path, error="timeout", ms=elapsed_ms))
        raise
    except requests.exceptions.RequestException as e:
        # includes HTTP 4xx/5xx (raise_for_status), connection refused, etc.
        status = getattr(getattr(e, "response", None), "status_code", "none")
        reqlog.warning(kv(path=path, error=type(e).__name__, status=status))
        raise


def _get_pickledb(autodump: bool = True):
    return pickledb.load("/pr/pr.db", autodump)


def _get_servers() -> list[dict]:
    query_params = {
        "includeHttps": 1,
        "includeRelay": 0,
        "includeIPv6": 0,
        "X-Plex-Client-Identifier": "".join(
            random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=24)
        ),
        "X-Plex-Platform-Version": "16.6",
        "X-Plex-Token": PLEX_TOKEN,
    }

    req = requests.get(
        url=f"https://clients.plex.tv/api/v2/resources?{urlencode(query_params)}",
        headers=HEADERS,
    )

    servers = {}
    for server in [server for server in req.json() if server["provides"] == "server"]:
        for conn in server["connections"]:
            if not conn["relay"] and not conn["local"] and not conn["IPv6"]:
                custom_access = False
                if "plex.direct" not in conn["uri"]:
                    custom_access = True

                    s = [c for c in server["connections"] if "plex.direct" in c["uri"]]

                    url = urlparse(conn["uri"])
                    server_ip = socket.gethostbyname(url.netloc.split(":")[0])
                    conn["uri"] = (
                        f"{server_ip.replace('.', '-')}.{s[0]['uri'].split('.')[1]}.plex.direct:{conn['port']}"
                    )

                uri = conn["uri"].split("://")[-1]
                node = uri.split(".")[1]
                ip = uri.split(".")[0].replace("-", ".")
                port = conn["port"]
                token = server["accessToken"]
                owned = server["owned"]

                if not servers.get(server["clientIdentifier"]) or custom_access:
                    servers[server["clientIdentifier"]] = {
                        "node": node,
                        "uri": uri,
                        "ip": ip,
                        "port": port,
                        "token": token,
                        "owned": owned,
                    }

    return list(servers.values())


def get_plex_playlists(plex_servers: list = None) -> None:
    # The ignore-list feature reads a named playlist from your OWN servers to hide
    # those titles from the listing. When no playlist is configured there is nothing
    # to do -- bail out so we never query any server needlessly.
    if not IGNORE_PLAYLIST:
        return

    pdb = _get_pickledb(autodump=True)  # pickledb (ignore-list); not the sqlite `db` module

    query_params = {
        "playlistType": "video",
        "includeCollections": 0,
        "includeExternalMedia": 1,
        "includeAdvanced": 1,
        "includeMeta": 1,
        "X-Plex-Client-Identifier": "".join(
            random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=24)
        ),
        "X-Plex-Platform-Version": "16.6",
        "X-Plex-Token": PLEX_TOKEN,
    }

    query_params_items = {
        "X-Plex-Container-Start": 0,
        "X-Plex-Container-Size": 120,
        "X-Plex-Client-Identifier": "".join(
            random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=24)
        ),
        "X-Plex-Platform-Version": "16.6",
        "X-Plex-Token": PLEX_TOKEN,
    }

    ignored_items = []

    for plex_server in [ps for ps in plex_servers if ps["owned"]]:
        playlists = requests.get(
            url=f"https://{plex_server['uri']}/playlists?{urlencode(query_params)}",
            timeout=15,
            headers=HEADERS,
        )

        for playlist in playlists.json()["MediaContainer"]["Metadata"]:
            if playlist["title"] == IGNORE_PLAYLIST:
                playlist_items = requests.get(
                    url=f"https://{plex_server['uri']}{playlist['key']}?{urlencode(query_params_items)}",
                    timeout=15,
                    headers=HEADERS,
                )

                for ignore_item in playlist_items.json()["MediaContainer"]["Metadata"]:
                    for media in ignore_item["Media"]:
                        for part in media["Part"]:
                            ignored_items.append(
                                part["file"]
                                .replace("/media/moviesextra/", "")
                                .replace("/media/showsextra/", "")
                                .strip("/")
                            )

                if pdb.exists("ignores"):
                    existing_ignore_items = pdb.get("ignores")
                    ignored_items = list(set(ignored_items + existing_ignore_items))

                pdb.set("ignores", ignored_items)


def reconcile_lazy_deletes() -> None:
    """Drain the lazy-delete reconcile queue and remove the corresponding rows from
    SQLite (the source of truth). nginx already dropped the Redis key on the 404 for
    an instant R: effect, but SQLite must also be cleaned or the next restart's
    rebuild_redis_from_db() would resurrect the deleted item. Each queue entry is
    "<node>|<media_type>|<file_path>". Deletes ONE item only (movie or episode),
    never the whole show. Re-enqueues itself on the refresh cadence."""
    drained = 0
    while True:
        entry = r.lpop("pr:lazydelete")
        if not entry:
            break
        try:
            node, media_type, file_path = entry.split("|", 2)
        except ValueError:
            continue
        # The serving path may carry a batchNN/ prefix (SHOWS_PER_BATCH); SQLite stores
        # the clean logical path without it, so strip it before the lookup.
        file_path = re.sub(r"^batch\d+/", "", file_path)
        if db.lazy_delete_path(node, media_type, file_path):
            drained += 1
    if drained:
        _log.get("lazydelete").info(kv(removed=drained))
    # keep draining periodically (cheap no-op when the queue is empty)
    rq_queue.enqueue_in(datetime.timedelta(minutes=15), "tasks.reconcile_lazy_deletes", retry=rq_retries)


def get_plex_servers() -> None:
    pdb = _get_pickledb(autodump=True)  # pickledb (ignore-list); not the sqlite `db` module
    rkey = "pr:servers"

    if not r.exists(rkey):
        time.sleep(random.randint(1, 20 if DEVELOPMENT else 60))

    if not r.exists(rkey):
        plex_servers = _get_servers()
        r.set(rkey, json.dumps(plex_servers))
        r.expire(rkey, int(REDIS_REFRESH_TTL / 3))
        rq_queue.enqueue_in(
            datetime.timedelta(seconds=int(REDIS_REFRESH_TTL / 3 + 60)), "tasks.get_plex_servers", retry=rq_retries
        )

        if IGNORE_PLAYLIST:
            rq_queue.enqueue("tasks.get_plex_playlists", at_front=True, retry=rq_retries, plex_servers=plex_servers)

        if not pdb.exists("ignores"):
            pdb.set("ignores", [])
    else:
        plex_servers = json.loads(r.get(rkey))

    now = int(time.time())
    for plex_server in [ps for ps in plex_servers if not ps["owned"]]:
        node = plex_server["node"]

        # ALWAYS restore the node connection info nginx needs to stream files. Redis is
        # wiped on every restart, so without this, playback 404s until the next crawl.
        # This is cheap (local Redis writes) and hits NO friend server. Fixes the
        # "playback broken for hours after restart" gap.
        r.set(f"pr:node:{node}:ip", plex_server["ip"])
        r.set(f"pr:node:{node}:port", str(plex_server["port"]))
        r.set(f"pr:node:{node}:token", plex_server["token"])

        # Gate the actual crawl on the SQLite last_crawl (survives restarts), NOT a
        # Redis throttle key (wiped every restart). Seeded-random threshold in
        # [REFRESH_HOURS_MIN, REFRESH_HOURS_MAX] stays stable across restarts until a
        # crawl actually happens -> 100 restarts/week do NOT cause 100 crawls.
        last = db.node_last_crawl_ts(node)
        if last is not None:
            # seeded so the gap is stable across restarts; randomized in SECONDS across
            # the [MIN, MAX]-hour window so refreshes don't recur at a fixed time of day.
            rng = random.Random(last)
            gap_secs = rng.randint(REFRESH_HOURS_MIN * 3600, REFRESH_HOURS_MAX * 3600)
            age = now - last
            if age < gap_secs:
                logger.info(
                    "skip "
                    + kv(node=node, reason="fresh", age_h=round(age / 3600, 1), gap_h=round(gap_secs / 3600, 1))
                )
                continue  # crawled recently -> restore-only, no requests to friend
            logger.info("due " + kv(node=node, age_h=round(age / 3600, 1), gap_h=round(gap_secs / 3600, 1)))
        else:
            logger.info("due " + kv(node=node, reason="never_crawled"))

        rq_queue.enqueue("tasks.get_plex_libraries", retry=rq_retries, kwargs={"plex_server": plex_server})


def get_plex_libraries(plex_server: dict = None) -> None:
    """Discover movie/show libraries on a server and enqueue one crawl job per
    library (single-job-per-library -- so each crawl sees the COMPLETE set and can
    safely do the deletion diff)."""
    mc = _plex_get(plex_server["uri"], "/library/sections", plex_server["token"], timeout=15)
    for library in mc.get("Directory", []):
        if library.get("type") == "movie":
            rq_queue.enqueue(
                "tasks.crawl_movie_library", retry=rq_retries, kwargs={"plex_server": plex_server, "library": library}
            )
        elif library.get("type") == "show":
            rq_queue.enqueue(
                "tasks.crawl_show_library", retry=rq_retries, kwargs={"plex_server": plex_server, "library": library}
            )


def _iter_section(uri: str, key: str, token: str, media_type_num: int):
    """Yield every item in a library section, paging with CONTAINER_SIZE. `type`
    is 1 (movies) or 2 (shows). Uses the Size=0 trick to learn totalSize cheaply."""
    start = 0
    while True:
        mc = _plex_get(
            uri,
            f"/library/sections/{key}/all",
            token,
            type=media_type_num,
            **{"X-Plex-Container-Start": start, "X-Plex-Container-Size": CONTAINER_SIZE},
        )
        items = mc.get("Metadata", [])
        for item in items:
            yield item
        total = mc.get("totalSize", mc.get("size", 0))
        start += CONTAINER_SIZE
        if start >= total or not items:
            break


def crawl_movie_library(plex_server: dict = None, library: dict = None) -> None:
    """Crawl ONE movie library end-to-end in a single job: page through all movies,
    upsert to SQLite, run the deletion diff against the COMPLETE fetched set, then
    swap Redis. Because we hold the full set here, deletion detection is correct."""
    node, uri, token = plex_server["node"], plex_server["uri"], plex_server["token"]
    started = time.monotonic()
    rows, seen = [], set()

    for movie in _iter_section(uri, library["key"], token, media_type_num=1):
        title_year = _title_year(movie)
        # collect all versions (Media x Part) of this movie that pass filters, THEN
        # assign unique paths -- a movie can have several versions (e.g. 4k + 1080)
        # that must each get a distinct file_path (and rating_key), not collide.
        versions = []
        for media in movie.get("Media", []):
            for part in media.get("Part", []):
                if not _passes_filters(part, media, MOVIE_MIN_SIZE):
                    continue
                if any(re.match(rf"{imt}", part["file"].split("/")[-1], flags=re.I) for imt in IGNORE_MOVIE_TEMPLATES):
                    continue
                versions.append({"part": part, **_file_attrs(part, media)})

        suffixes = _version_suffixes(versions)
        for v, suffix in zip(versions, suffixes):
            part = v["part"]
            rk = part["key"]  # per-file key -> unique per version (movie ratingKey is shared)
            seen.add(rk)
            rows.append(
                {
                    "server_node": node,
                    "media_type": "movies",
                    "rating_key": rk,
                    "show_key": None,
                    "file_path": f"{title_year}/{title_year}{suffix}{_ext(part['file'])}",
                    "plex_key": part["key"],
                    "title": title_year,
                    "updated_at": movie.get("updatedAt"),
                    "added_at": movie.get("addedAt"),
                    "size": v["size"],
                    "duration": v["duration"],
                    "container": v["container"],
                    "resolution": v["resolution"],
                }
            )

    db.upsert_media_batch(rows)
    # deletion diff: anything in SQLite for this node but NOT seen in this complete
    # crawl was removed upstream. (We only reach here if the full crawl succeeded --
    # an exception mid-crawl aborts the job and leaves the old snapshot intact.)
    stale = db.get_rating_keys(node, "movies") - seen
    db.delete_media_keys(node, "movies", list(stale))
    swap_redis_listing(node, "movies")
    db.record_crawl(node, "movies", status="ok", is_full=True)
    logger.info(
        "movies " + kv(node=node, kept=len(seen), deleted=len(stale), secs=round(time.monotonic() - started, 1))
    )


def crawl_show_library(plex_server: dict = None, library: dict = None) -> None:
    """Crawl ONE show library incrementally: list shows cheaply (leafCount +
    updatedAt, NO episode fetch), then only deep-fetch (allLeaves) shows that are
    new or changed. Unchanged shows cost ZERO episode requests, which keeps the
    crawl bounded even for very large TV libraries.

    Weekly reconciliation: if the last FULL crawl was longer ago than a RANDOM
    threshold in [FULL_CRAWL_DAYS_MIN, FULL_CRAWL_DAYS_MAX] days, run in force_full
    mode -- deep-fetch every show regardless of leafCount/updatedAt, to heal any
    drift where Plex changed content without bumping updatedAt. The threshold is
    randomized (not an exact 7d) so the heavy crawl doesn't look like clockwork
    automation in the friend's logs, and seeded from last_full so it stays stable
    across the many intermediate 5-12h checks (only re-rolls after a full crawl)."""
    node, uri, token = plex_server["node"], plex_server["uri"], plex_server["token"]
    started = time.monotonic()
    last_full = db.last_full_crawl_ts(node, "shows")
    if last_full is None:
        force_full = True
    else:
        # deterministic per last_full: same threshold on every check until next full crawl.
        # Randomize in SECONDS across the [MIN, MAX]-day window (not whole days) so the
        # heavy crawl doesn't recur at the same TIME OF DAY each cycle -- a fixed
        # time-of-day would be an obvious fingerprint in the friend's logs even if the
        # day varies. This spreads it across both date and clock time.
        rng = random.Random(last_full)
        threshold_secs = rng.randint(FULL_CRAWL_DAYS_MIN * 86400, FULL_CRAWL_DAYS_MAX * 86400)
        force_full = (int(time.time()) - last_full) > threshold_secs

    prior = db.get_show_state(node)  # {show_key: {leaf_count, updated_at}}
    seen_shows = set()
    changed = 0

    for show in _iter_section(uri, library["key"], token, media_type_num=2):
        show_key = str(show["ratingKey"])
        seen_shows.add(show_key)
        leaf = show.get("leafCount") or 0
        upd = show.get("updatedAt") or 0
        was = prior.get(show_key)
        # deep-fetch if: weekly full rebuild, OR new, OR episode count/updatedAt changed
        if force_full or was is None or was["leaf_count"] != leaf or (was["updated_at"] or 0) != upd:
            changed += 1
            rq_queue.enqueue(
                "tasks.get_show_leaves",
                retry=rq_retries,
                kwargs={
                    "plex_server": plex_server,
                    "show_key": show_key,
                    "show_title": show.get("title", "Unknown"),
                    "leaf_count": leaf,
                    "updated_at": upd,
                },
            )

    # whole shows that vanished upstream -> drop their episodes + state
    gone = set(prior) - seen_shows
    deleted_eps = db.delete_shows(node, list(gone))
    # finalize now for deletions/unchanged; changed shows each swap when they land
    swap_redis_listing(node, "shows")
    db.record_crawl(node, "shows", status="ok", is_full=force_full)
    logger.info(
        "shows "
        + kv(
            node=node,
            total=len(seen_shows),
            changed=changed,
            full=force_full,
            removed_shows=len(gone),
            removed_eps=deleted_eps,
            secs=round(time.monotonic() - started, 1),
        )
    )


def get_show_leaves(
    plex_server: dict = None, show_key: str = None, show_title: str = None, leaf_count: int = 0, updated_at: int = 0
) -> None:
    """Fetch ALL episodes of ONE show in a single allLeaves request, replace that
    show's episodes in SQLite atomically, update its change-detection state, and
    swap Redis. One request per changed show."""
    node, uri, token = plex_server["node"], plex_server["uri"], plex_server["token"]
    mc = _plex_get(uri, f"/library/metadata/{show_key}/allLeaves", token)
    show = show_title or mc.get("grandparentTitle") or "Unknown"
    rows = []

    for ep in mc.get("Metadata", []):
        s_no = ep.get("parentIndex", 0)
        e_no = ep.get("index", 0)
        ep_title = ep.get("title", "")
        fname = f"{show} - S{int(s_no):02d}E{int(e_no):02d}"
        if ep_title:
            fname += f" - {ep_title}"

        # collect this episode's versions, then assign unique paths (same multi-version
        # handling as movies -- an episode can also have >1 version).
        versions = []
        for media in ep.get("Media", []):
            for part in media.get("Part", []):
                if not _passes_filters(part, media, EPISODE_MIN_SIZE):
                    continue
                if any(re.match(rf"{imt}", part["file"].lower(), flags=re.I) for imt in IGNORE_EPISODE_TEMPLATES):
                    continue
                versions.append({"part": part, **_file_attrs(part, media)})

        suffixes = _version_suffixes(versions)
        for v, suffix in zip(versions, suffixes):
            part = v["part"]
            rows.append(
                {
                    "server_node": node,
                    "media_type": "shows",
                    "rating_key": part["key"],
                    "show_key": show_key,
                    "file_path": f"{show}/Season {int(s_no):02d}/{fname}{suffix}{_ext(part['file'])}",
                    "plex_key": part["key"],
                    "title": show,
                    "updated_at": ep.get("updatedAt"),
                    "added_at": ep.get("addedAt"),
                    "size": v["size"],
                    "duration": v["duration"],
                    "container": v["container"],
                    "resolution": v["resolution"],
                }
            )

    db.replace_show_episodes(node, show_key, rows)
    db.upsert_show_state(node, show_key, leaf_count, updated_at)
    swap_redis_listing(node, "shows")
    logger.debug("show_leaves " + kv(node=node, show=show, episodes=len(rows)))


def _title_year(item: dict) -> str:
    """'Title (Year)' with a graceful fallback when year is missing."""
    year = item.get("year")
    return f"{item['title']} ({year})" if year else item["title"]


def _ext(file_path: str) -> str:
    """File extension including the dot, e.g. '.mkv'. Empty string if none."""
    base = file_path.rsplit("/", 1)[-1]
    return f".{base.rsplit('.', 1)[-1]}" if "." in base else ""


def _version_suffixes(versions: list[dict]) -> list[str]:
    """Given the versions (each {'resolution','container',...}) of ONE title, return a
    parallel list of filename suffixes that make every version's path UNIQUE.

    - single version  -> [""]                      (clean: no suffix)
    - distinct resolutions -> [" - 4k", " - 1080"] (meaningful to the viewer; Plex
                                                     treats these as multi-version)
    - same resolution / missing -> a counter is appended so paths never collide
      (" - 1080", " - 1080 (2)"), and as a final guarantee any remaining duplicate
      suffix gets "(n)" until the whole list is unique.
    """
    if len(versions) <= 1:
        return [""]

    # base tag from resolution when present, else generic "v{i}"
    tags = []
    for i, v in enumerate(versions, start=1):
        res = v.get("resolution")
        tags.append(f" - {res}" if res else f" - v{i}")

    # guarantee uniqueness: append " (n)" to any repeated tag
    seen: dict[str, int] = {}
    unique = []
    for tag in tags:
        if tag in seen:
            seen[tag] += 1
            unique.append(f"{tag} ({seen[tag]})")
        else:
            seen[tag] = 1
            unique.append(tag)
    return unique


def _passes_filters(part: dict, media: dict, min_size_mb: int) -> bool:
    """Shared size/resolution/extension gate for movies and episodes."""
    if media.get("videoResolution") in IGNORE_RESOLUTIONS:
        return False
    if not part.get("key") or not part.get("file"):
        return False
    if part.get("container") in IGNORE_EXTENSIONS:
        return False
    if part.get("size", 1) / 1_000_000 < min_size_mb:
        return False
    return True


def _file_attrs(part: dict, media: dict) -> dict:
    """Playback/filesystem attributes captured from a Part/Media, shared by movies
    and episodes. Stored in SQLite (size also drives nginx's local HEAD response)."""
    return {
        "size": part.get("size"),
        "duration": part.get("duration") or media.get("duration"),
        "container": part.get("container") or media.get("container"),
        "resolution": media.get("videoResolution"),
    }


def swap_redis_listing(server_node: str, media_type: str) -> int:
    """Rebuild the Redis serving listing for one server+type from SQLite.

    Reads the complete current set from SQLite, then in a SINGLE Redis pipeline
    deletes the old pr:files:<type>/<node>/* keys and writes the new ones. Doing the
    delete+write in one pipeline means readers only ever see a complete snapshot,
    never a half-built listing. Listing keys carry no TTL -- they live until the
    next swap replaces them. Returns the number of keys written.
    """
    rows = db.get_media(server_node, media_type)  # [{file_path, plex_key, size}, ...]

    # Optional batchNN/ prefix for shows: keep it in the SERVING listing only. SQLite
    # file_path stays the clean logical path ("<show>/Season .../ep"); the batch folder
    # is inserted here when writing Redis keys, driven by the persisted stable
    # show->batch map. Movies are never batched.
    batch_of: dict[str, int] = {}
    if media_type == "shows":
        if SHOWS_PER_BATCH > 0:
            show_names = sorted({row["file_path"].split("/", 1)[0] for row in rows})
            batch_of = db.sync_show_batches(server_node, show_names, SHOWS_PER_BATCH)
        else:
            db.clear_show_batches(server_node)  # disabled -> re-enable starts fresh

    old_keys = list(r.scan_iter(f"pr:files:{media_type}/{server_node}/*"))
    pipe = r.pipeline()
    if old_keys:
        pipe.delete(*old_keys)
    for row in rows:
        file_path = row["file_path"]
        if batch_of:
            show_name = file_path.split("/", 1)[0]
            file_path = f"batch{batch_of[show_name]:02d}/{file_path}"
        key = f"pr:files:{media_type}/{server_node}/{file_path}"
        # value = "<plex_key>|<size>". nginx splits on "|": the plex_key half is what
        # it proxies to on playback (GET), and the size half lets it answer HEAD/size
        # lookups locally without a round-trip to the origin server.
        pipe.set(key, f"{row['plex_key']}|{row['size'] if row['size'] is not None else ''}")
    pipe.execute()
    return len(rows)


def rebuild_redis_from_db() -> int:
    """Repopulate ALL of Redis from SQLite (used on restart when data is fresh).
    Returns total keys written. Groups rows by (node, type) so each swap is atomic."""
    db.init_db()
    groups: dict[tuple[str, str], bool] = {}
    for row in db.get_all_media():
        groups[(row["server_node"], row["media_type"])] = True
    total = 0
    for node, media_type in groups:
        total += swap_redis_listing(node, media_type)
    _log.get("startup").info("rebuild_redis " + kv(items=total, groups=len(groups)))
    return total


def startup() -> None:
    """Entry point enqueued by the web app on boot (replaces the old blind flushdb).

    Restart-safe design:
      1. If SQLite has media, immediately rebuild the Redis listing from it -> R: is
         populated instantly on restart (no wait, no crawl).
      2. ALWAYS run get_plex_servers now. It restores the per-node connection info
         nginx needs for playback (Redis is wiped on restart) and then decides -- from
         the SQLite-backed last_crawl, which survives restarts -- whether a 5-12h
         refresh (or the 6-9 day full crawl) is actually due. So restarting 100x/week
         does NOT cause 100 crawls; friends are hit only on the real cadence.
    """
    slog = _log.get("startup")
    slog.info("boot " + kv(has_media=db.has_any_media()))
    db.init_db()
    # start the lazy-delete reconciler loop (drains pr:lazydelete -> sqlite cleanup)
    rq_queue.enqueue("tasks.reconcile_lazy_deletes", job_id="reconcile_lazy_deletes", retry=rq_retries)

    if db.has_any_media():
        rebuild_redis_from_db()

    # get_plex_servers restores node conn info + gates the crawl on SQLite last_crawl
    rq_queue.enqueue("tasks.get_plex_servers", job_id="get_plex_servers", retry=rq_retries)
