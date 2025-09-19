from __future__ import annotations
from typing import Dict, List
from sqlalchemy.orm import Session

from .models import Artist, Release, ArtistRelease, Track
from .spotify_client import (
    fetch_artist,
    fetch_artist_releases_df,
    hydrate_album,
)

# small helpers
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
      - prefer album_group 'album' (over 'single', 'compilation', 'appears_on')
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
    
def ingest_artist(db: Session, sp, artist_id: str):
    """
    fetch artist + all releases from spotify, collapse variants,
    store albums + tracks + upc, and tag artist-release role.
    """
    # 1) artist basics
    artist_obj = fetch_artist(sp, artist_id)
    artist_img = (artist_obj.get("images") or [{}])[0].get("url")
    upsert_artist_row(db, artist_id, artist_obj["name"], artist_img)
    db.commit()

    # 2) releases as dataframe (flat)
    df = fetch_artist_releases_df(sp, artist_id)
    if df.empty:
        return

    # 3) build variant mapping per canonical_key
    variant_of: Dict[str, str] = {}
    canonical_ids: List[str] = []

    for key, grp in df.groupby("canonical_key"):
        canon = pick_canonical_df(grp)
        canon_id = canon["id"]
        canonical_ids.append(canon_id)
        for rid in grp["id"]:
            if rid != canon_id:
                variant_of[rid] = canon_id

    # 4) upsert releases + link artist releases with role
    #    role: 'primary' unless album_group == 'appears_on'
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

        # artist <-> release role
        link_role = "primary" if (row.group or "") != "appears_on" else "appears_on"

        # attach link if missing; else update role if changed
        # NOTE: accessing rel.artists is fine here because rel is attached to `db`
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

    # 5) hydrate albums: upc + full track list (with isrc)
    #    this is api-heavy; keep, because you want per-track ratings later
    album_ids = df["id"].tolist()
    for rid in album_ids:
        alb, tracks = hydrate_album(sp, rid)
        rel = db.get(Release, rid)
        if not rel:
            continue

        # upc
        ext_ids = alb.get("external_ids") or {}
        rel.upc = ext_ids.get("upc")

        # reset tracks to keep in sync with spotify
        # relationship is managed; clearing then appending is fine
        rel.tracks.clear()
        for t in tracks:
            if not t.get("id"):
                continue
            rel.tracks.append(Track(
                id=t["id"],
                release_id=rid,
                title=t.get("title") or "",
                disc_number=_safe_int(t.get("disc_number")) or 1,
                track_number=_safe_int(t.get("track_number")) or 1,
                duration_ms=_safe_int(t.get("duration_ms")),
                explicit=bool(t.get("explicit", False)),
                isrc=t.get("isrc"),
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
