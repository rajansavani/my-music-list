from __future__ import annotations

from typing import Dict, List, Tuple
import re
import pandas as pd

# words we strip when building a "canonical" title to collapse variants
VARIANT_WORDS = re.compile(
    r"\b(deluxe|expanded|remaster(ed)?|clean|bonus|anniversary|commentary|instrumental|edit|explicit)\b",
    re.I,
)

def normalize_title(title: str) -> str:
    # lower, remove bracket noise, drop variant keywords, collapse whitespace
    t = title.lower()
    t = re.sub(r"[\(\)\[\]\{\}]", " ", t)
    t = VARIANT_WORDS.sub("", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def canonical_key(title: str | None, release_date: str | None, group: str | None) -> str:
    # key is normalized title + year + album_group; keeps singles/separate groupings distinct
    tt = normalize_title(title or "")
    year = (release_date or "")[:4]
    gg = group or ""
    return f"{tt}::{year}::{gg}"

# raw spotify client calls

def fetch_artist(sp, artist_id: str) -> Dict:
    # full artist object (name, images, followers, etc.)
    return sp.artist(artist_id)

def fetch_artist_releases(sp, artist_id: str, limit: int = 50) -> List[Dict]:
    items: List[Dict] = []
    offset = 0
    while True:
        resp = sp.artist_albums(
            artist_id,
            album_type="album,single,compilation,appears_on",
            limit=min(limit, 50),
            offset=offset,
            country="US",
        )
        batch = resp.get("items", []) or []
        items.extend(batch)
        if not resp.get("next"):
            break
        offset += len(batch)
        if limit and len(items) >= limit:
            break
    return items

def fetch_artist_releases_df(sp, artist_id: str, limit: int = 1000) -> pd.DataFrame:
    rows: List[Dict] = []
    offset = 0
    fetched = 0
    while True:
        resp = sp.artist_albums(
            artist_id,
            album_type="album,single,compilation,appears_on",
            limit=50,
            offset=offset,
            country="US",
        )
        batch = resp.get("items", []) or []
        for r in batch:
            rows.append({
                "id": r["id"],
                "title": r["name"],
                "group": r.get("album_group"),              # album | single | compilation | appears_on
                "type": r.get("album_type"),                # album | single | compilation
                "release_date": r.get("release_date"),
                "precision": r.get("release_date_precision"),
                "total_tracks": r.get("total_tracks"),
                "cover": (r.get("images") or [{}])[0].get("url"),
            })
        fetched += len(batch)
        if not resp.get("next") or fetched >= limit:
            break
        offset += len(batch)

    df = pd.DataFrame(rows)
    if not df.empty:
        df["canonical_key"] = df.apply(
            lambda row: canonical_key(row["title"], row["release_date"], row["group"]),
            axis=1
        )
    return df

def hydrate_album(sp, album_id: str) -> Tuple[Dict, List[Dict]]:
    """
    returns (album_obj_with_upc, tracks_list_with_isrc)
    shape matches your earlier ingest.py expectations.
    - album dict includes external_ids (e.g., upc)
    - tracks list entries include: id, title, disc_number, track_number, duration_ms, explicit, isrc
    """
    # full album (has tracks paging embedded)
    alb = sp.album(album_id)

    # gather track ids from the album's first page
    tr_items = alb.get("tracks", {}).get("items", []) or []
    track_ids: List[str] = [t["id"] for t in tr_items if t.get("id")]

    # fetch additional pages of tracks if any
    # note: sp.album_tracks could be used, but alb already returns first page so we follow "next" if present
    next_url = alb.get("tracks", {}).get("next")
    offset = len(tr_items)
    while next_url:
        resp = sp.album_tracks(album_id, limit=50, offset=offset)
        more = resp.get("items", []) or []
        tr_items.extend(more)
        track_ids.extend([t["id"] for t in more if t.get("id")])
        next_url = resp.get("next")
        offset += len(more)

    # batch fetch full track objects to get isrcs
    isrc_map: Dict[str, str] = {}
    for i in range(0, len(track_ids), 50):
        chunk = track_ids[i:i+50]
        full = sp.tracks(chunk).get("tracks", []) if chunk else []
        for tr in full or []:
            if tr and tr.get("id"):
                ext = tr.get("external_ids") or {}
                isrc_map[tr["id"]] = ext.get("isrc")

    tracks_out: List[Dict] = []
    for t in tr_items:
        tracks_out.append({
            "id": t.get("id"),
            "title": t.get("name"),
            "disc_number": t.get("disc_number", 1),
            "track_number": t.get("track_number", 1),
            "duration_ms": t.get("duration_ms"),
            "explicit": t.get("explicit", False),
            "isrc": isrc_map.get(t.get("id")),
        })

    return alb, tracks_out
