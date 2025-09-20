# all comments in lowercase per your style

from __future__ import annotations
from typing import Dict, List, Tuple
import re
import pandas as pd

# words we strip when building a "canonical" title to collapse album variants
VARIANT_WORDS = re.compile(
    r"\b(deluxe|expanded|remaster(ed)?|clean|bonus|anniversary|commentary|instrumental|edit|explicit)\b",
    re.I,
)

def normalize_title(title: str) -> str:
    t = title.lower()
    t = re.sub(r"[\(\)\[\]\{\}]", " ", t)
    t = VARIANT_WORDS.sub("", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def canonical_album_key(title: str | None, release_date: str | None, group: str | None) -> str:
    tt = normalize_title(title or "")
    year = (release_date or "")[:4]
    gg = group or ""
    return f"{tt}::{year}::{gg}"

# ---------- raw spotify fetches ----------

def fetch_artist(sp, artist_id: str) -> Dict:
    return sp.artist(artist_id)

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
                "canonical_key": canonical_album_key(r.get("name"), r.get("release_date"), r.get("album_group")),
            })
        fetched += len(batch)
        if not resp.get("next") or fetched >= limit:
            break
        offset += len(batch)

    return pd.DataFrame(rows)

def hydrate_album(sp, album_id: str) -> Tuple[Dict, List[Dict]]:
    # full album object (contains tracks page 1)
    alb = sp.album(album_id)

    tr_items = alb.get("tracks", {}).get("items", []) or []
    track_ids = [t["id"] for t in tr_items if t.get("id")]

    # more pages
    next_url = alb.get("tracks", {}).get("next")
    offset = len(tr_items)
    while next_url:
        resp = sp.album_tracks(album_id, limit=50, offset=offset)
        more = resp.get("items", []) or []
        tr_items.extend(more)
        track_ids.extend([t["id"] for t in more if t.get("id")])
        next_url = resp.get("next")
        offset += len(more)

    # batch fetch to get ISRCs
    isrc_map: Dict[str, str] = {}
    for i in range(0, len(track_ids), 50):
        chunk = track_ids[i:i+50]
        full = sp.tracks(chunk).get("tracks", []) if chunk else []
        for tr in full or []:
            if tr and tr.get("id"):
                isrc_map[tr["id"]] = (tr.get("external_ids", {}) or {}).get("isrc")

    # build track dicts with artist lists
    tracks_out: List[Dict] = []
    for t in tr_items:
        artists = t.get("artists") or []
        tracks_out.append({
            "id": t.get("id"),
            "title": t.get("name"),
            "disc_number": t.get("disc_number", 1),
            "track_number": t.get("track_number", 1),
            "duration_ms": t.get("duration_ms"),
            "explicit": t.get("explicit", False),
            "isrc": isrc_map.get(t.get("id")),
            "artist_ids": [a.get("id") for a in artists if a.get("id")],
            "artist_names": [a.get("name") for a in artists if a.get("name")],
        })

    return alb, tracks_out
