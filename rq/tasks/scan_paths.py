"""Pure helpers for the targeted-scan feature: path transforms, catalog/Plex diff,
and the scan time-window check. No IO -- unit-testable offline.

Two path forms are converted here, both driven by a library's live `Location` root
(never a hardcoded drive letter):
  * catalog form:  mount-relative, '/'-separated, e.g. "movies/<guid>/Title/file.mkv"
  * windows form:  what Plex reports/expects, e.g. r"R:\\movies\\<guid>\\Title\\file.mkv"
"""

from datetime import datetime, time
from zoneinfo import ZoneInfo


def plex_part_to_catalog(part_file: str, mount_root: str) -> str | None:
    """Plex Windows Part path -> mount-relative catalog form, or None if not under
    mount_root. mount_root is the drive/mount level (e.g. "R:"), so the first segment
    after it ("movies"/"shows") and any batchNN segment are preserved verbatim.
    e.g. (r"R:\\movies\\g\\T\\T.mkv", "R:") -> "movies/g/T/T.mkv"."""
    root = mount_root.rstrip("\\/")
    if not part_file.startswith(root):
        return None
    rel = part_file[len(root) :].lstrip("\\/")
    return rel.replace("\\", "/")


def folder_of(catalog_path: str) -> str:
    """Parent folder of a catalog file path (drop last segment)."""
    return catalog_path.rsplit("/", 1)[0]


def catalog_to_windows(folder: str, mount_root: str) -> str:
    """Catalog folder -> Windows path under mount_root. Inverse of plex_part_to_catalog.
    The 'movies'/'shows' segment is already part of `folder`, so we just join.
    e.g. ("movies/g/T", "R:") -> r"R:\\movies\\g\\T"."""
    root = mount_root.rstrip("\\/")
    return root + "\\" + folder.replace("/", "\\")


def diff_folders(catalog_files: set[str], plex_files: set[str], in_scope) -> dict[str, str]:
    """Roll files up to folders and assign a status to each catalog folder:
      * 'scanned' if every catalog file in the folder is present in plex_files
      * 'blocked' if the folder is out of Plex library scope (in_scope(folder) False)
      * 'pending' otherwise (in scope, at least one file missing)
    `in_scope` is a callable folder -> bool."""
    by_folder: dict[str, list[str]] = {}
    for f in catalog_files:
        by_folder.setdefault(folder_of(f), []).append(f)
    out: dict[str, str] = {}
    for folder, files in by_folder.items():
        if all(f in plex_files for f in files):
            out[folder] = "scanned"
        elif not in_scope(folder):
            out[folder] = "blocked"
        else:
            out[folder] = "pending"
    return out


def in_window(now_local: datetime, start_hhmm: str, end_hhmm: str) -> bool:
    """True if now_local's clock time is within [start, end] (same-day, start<end)."""
    sh, sm = (int(x) for x in start_hhmm.split(":"))
    eh, em = (int(x) for x in end_hhmm.split(":"))
    t = now_local.timetz()
    return time(sh, sm, tzinfo=t.tzinfo) <= t <= time(eh, em, tzinfo=t.tzinfo)


def now_in_tz(tz_name: str) -> datetime:
    """Current tz-aware datetime in tz_name."""
    return datetime.now(ZoneInfo(tz_name))
