# pandas-first ingest + variant collapse, then write into orm

from __future__ import annotations
from typing import Dict, List
from sqlalchemy.orm import Session

from .models import Artist, Release, ArtistRelease, Track
from .spotify_client import (
    fetch_artist,
    fetch_artist_releases_df,
    hydrate_album,
    normalize_title,  # used for canonical track key
)

# --- helpers -----------------------------------------------------------------

def upsert_artist_row(db: Session, artist_id: str, name: str, image: str | None):
    row = db.get(Artist, artist_id)
    if row is None:
        row = Artist(id=artist_id, name=name, image=image)
        db.add(row)
    else:
        row.name, row.image = name, image

def pick_canonical_df(group_df):
    """
    choose canonical release in a group:
      - prefer album_group 'album'
      - then highest total_tracks
      - then lowest id (stable tiebreak)
    """
    grp = group_df.copy()
    grp["group_rank"] = grp["group"].fillna("zzz").map(lambda g: 0 if g == "album" else 1)
    grp["tracks_rank"] = grp["total_tracks"].fillna(0).astype(int)
    row = grp.sort_values(
        by=["group_rank", "tracks_rank", "id"],
        ascending=[True, False, True],
    ).iloc[0]
    return row

def _safe_int(x):
    try:
        return int(x) if x is not None else None
    except Exception:
        return None

# duplicate track canonical key (heuristic):
# normalized title + duration seconds bucket + sorted artist ids
def canonical_track_key(title: str | None, duration_ms: int | None, artist_ids: list[str] | None) -> str:
    t = normalize_title(title or "")
    d_sec = int((duration_ms or 0) // 1000)  # bucket to seconds to avoid ms drift
    ids_sorted = ",".join(sorted(artist_ids or []))
    return f"{t}::{d_sec}::{ids_sorted}"

# --- main ingest -------------------------------------------------------------

def ingest_artist(db: Session, sp, artist_id: str):
    """
    fetch artist + releases from spotify, collapse variants,
    store albums + tracks (+track artists), and tag artist-release role.
    also compute track canonical keys for duplicate propagation.
    """
    # 1) artist
    artist_obj = fetch_artist(sp, artist_id)
    artist_img = (artist_obj.get("images") or [{}])[0].get("url")
    upsert_artist_row(db, artist_id, artist_obj["name"], artist_img)
    db.commit()

    # 2) releases df
    df = fetch_artist_releases_df(sp, artist_id)
    if df.empty:
        return

    # 3) variant mapping per df.canonical_key
    variant_of: Dict[str, str] = {}
    for key, grp in df.groupby("canonical_key"):
        canon = pick_canonical_df(grp)
        canon_id = canon["id"]
        for rid in grp["id"]:
            if rid != canon_id:
                variant_of[rid] = canon_id

    # 4) upsert releases + link artist with role
    for row in df.itertuples(index=False):
        rel = db.get(Release, row.id)
        if rel is None:
            rel = Release(
                id=row.id,
                title=row.title,
                type=row.type,
                group=row.group,
                release_date=row.release_date,
                date_precision=row.precision,
                total_tracks=_safe_int(row.total_tracks),
                cover=row.cover,
                source="spotify",
            )
            db.add(rel)
        else:
            rel.title = row.title
            rel.type = row.type
            rel.group = row.group
            rel.release_date = row.release_date
            rel.date_precision = row.precision
            rel.total_tracks = _safe_int(row.total_tracks)
            rel.cover = row.cover

        link_role = "primary" if (row.group or "") != "appears_on" else "appears_on"
        found = None
        for ar in rel.artists:
            if ar.artist_id == artist_id and ar.release_id == row.id:
                found = ar
                break
        if found is None:
            db.add(ArtistRelease(artist_id=artist_id, release_id=row.id, role=link_role))
        else:
            if found.role != link_role:
                found.role = link_role

    db.commit()

    # 5) hydrate albums: upc + tracks w/ artists + canonical track key
    album_ids = df["id"].tolist()
    for rid in album_ids:
        alb, tracks = hydrate_album(sp, rid)
        rel = db.get(Release, rid)
        if not rel:
            continue

        # upc
        ext_ids = alb.get("external_ids") or {}
        rel.upc = ext_ids.get("upc")

        # reset tracks and append fresh
        rel.tracks.clear()
        for t in tracks:
            if not t.get("id"):
                continue
            ids = t.get("artist_ids") or []
            names = t.get("artist_names") or []
            primary_id = ids[0] if ids else None
            canon_key = canonical_track_key(t.get("title"), t.get("duration_ms"), ids)

            rel.tracks.append(Track(
                id=t["id"],
                release_id=rid,
                title=t.get("title") or "",
                disc_number=_safe_int(t.get("disc_number")) or 1,
                track_number=_safe_int(t.get("track_number")) or 1,
                duration_ms=_safe_int(t.get("duration_ms")),
                explicit=bool(t.get("explicit", False)),
                isrc=t.get("isrc"),
                artist_ids_csv=",".join(ids),
                artist_names_csv=",".join(names),
                primary_artist_id=primary_id,
                canonical_key=canon_key,
            ))
    db.commit()

    # 6) mark variants
    for rid in album_ids:
        rel = db.get(Release, rid)
        if not rel:
            continue
        if rid in variant_of:
            rel.is_variant = True
            rel.variant_of = variant_of[rid]
        else:
            rel.is_variant = False
            rel.variant_of = None
    db.commit()
