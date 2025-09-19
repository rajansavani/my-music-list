# streamlit_app.py
# minimal app with artist completion meters, album/track ratings, and safe db sessions

import os
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from sqlalchemy import func

from src.db import init_db, SessionLocal
from src.models import (
    Artist, Release, ArtistRelease,
    UserReleaseState, UserTrackState, Track
)
from src.ingest import ingest_artist

# ---------- setup ----------
st.set_page_config(page_title="my music list", page_icon="🎶")
load_dotenv(dotenv_path=Path(__file__).parent / ".env")
init_db()

CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
# important: keep redirect on a different port than streamlit (8501) to avoid conflicts
REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI", "http://127.0.0.1:8888/callback")
CACHE_PATH = Path(__file__).parent / ".spotipyoauthcache"

auth_manager = SpotifyOAuth(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
    scope="user-read-email",
    cache_path=str(CACHE_PATH),
)
sp = spotipy.Spotify(auth_manager=auth_manager)

def db():
    return SessionLocal()

# ---------- helpers: ratings + metrics ----------

def rating_color(score: int | float | None) -> str:
    """0 -> red, 5 -> yellow, 10 -> green. returns hex color."""
    if score is None:
        return "#666666"
    s = max(0.0, min(10.0, float(score)))
    if s <= 5.0:
        # red to yellow
        t = s / 5.0
        r, g, b = 255, int(255 * t), 0
    else:
        # yellow to green
        t = (s - 5.0) / 5.0
        r, g, b = int(255 * (1 - t)), 255, 0
    return f"#{r:02x}{g:02x}{b:02x}"

def rating_badge(label: str, score: float | None):
    col = rating_color(score)
    txt = "—" if score is None else f"{score:.1f}"
    st.markdown(
        f'<span style="display:inline-block;padding:4px 8px;border-radius:10px;'
        f'background:{col}20;border:1px solid {col};color:{col};font-weight:600">'
        f'{label}: {txt}</span>',
        unsafe_allow_html=True
    )

def album_display_rating(s, album_id: str) -> float | None:
    """
    album rating rule:
      - if an explicit album override exists -> use it
      - elif any track ratings exist -> average them
      - else -> None
    """
    rs = s.get(UserReleaseState, album_id)
    if rs and rs.rating is not None:
        return float(rs.rating)
    avg = (
        s.query(func.avg(UserTrackState.rating))
        .join(Track, Track.id == UserTrackState.track_id)
        .filter(Track.release_id == album_id, UserTrackState.rating.isnot(None))
        .scalar()
    )
    return float(avg) if avg is not None else None

def artist_completion(s, artist_id: str, primary_only: bool = True) -> tuple[int, int, float]:
    """
    returns (listened_count, total_releases, percentage) at the release level.
    filters canonical releases only (variants hidden).
    """
    relq = (
        s.query(Release.id)
        .join(ArtistRelease, ArtistRelease.release_id == Release.id)
        .filter(ArtistRelease.artist_id == artist_id)
        .filter((Release.is_variant == False) | (Release.is_variant.is_(None)))
    )
    if primary_only:
        relq = relq.filter(ArtistRelease.role == "primary")

    release_ids = [rid for (rid,) in relq.all()]
    total = len(release_ids)
    if total == 0:
        return 0, 0, 0.0

    listened = (
        s.query(func.count(UserReleaseState.release_id))
        .filter(UserReleaseState.release_id.in_(release_ids), UserReleaseState.listened == True)
        .scalar()
    ) or 0

    pct = (listened / total) * 100.0
    return listened, total, pct

# ---------- ui ----------
st.title("discography tracker 🎶")

# auth check
try:
    me = sp.me()
    st.caption(f"logged in as **{me.get('display_name') or me.get('id')}**")
except Exception:
    st.error("spotify auth failed; verify client id/secret and redirect uri, then restart")
    st.stop()

# add / sync artists
st.subheader("add artists")
q = st.text_input("🔍 search for an artist on spotify")
if q:
    res = sp.search(q=q, type="artist", limit=5)
    for a in res["artists"]["items"]:
        aid, name = a["id"], a["name"]
        img = (a.get("images") or [{}])[0].get("url")
        cols = st.columns([1, 3, 2])
        with cols[0]:
            if img:
                st.image(img, width=72)
        with cols[1]:
            st.write(f"**{name}**")
        with cols[2]:
            if st.button("add & sync", key=f"addsync_{aid}"):
                with db() as s:
                    ingest_artist(s, sp, aid)
                st.success(f"synced {name}")

st.divider()

# list artists
st.subheader("📀 your artists")
with db() as s:
    artists = s.query(Artist).order_by(Artist.name.asc()).all()

if not artists:
    st.info("no artists yet — search above to add and sync.")
else:
    for artist in artists:
        st.markdown(f"## {artist.name}")
        if artist.image:
            st.image(artist.image, width=96)

        # completion meters
        with db() as s:
            p_listened, p_total, p_pct = artist_completion(s, artist.id, primary_only=True)
            a_listened, a_total, a_pct = artist_completion(s, artist.id, primary_only=False)

        c1, c2 = st.columns(2)
        with c1:
            st.write("**primary discography**")
            st.progress(int(p_pct))
            st.caption(f"{p_listened}/{p_total} • {p_pct:.0f}%")
        with c2:
            st.write("**all releases (incl. appears_on)**")
            st.progress(int(a_pct))
            st.caption(f"{a_listened}/{a_total} • {a_pct:.0f}%")

        # toggle which releases to show
        view_mode = st.radio(
            "view",
            ["primary only", "all releases"],
            horizontal=True,
            key=f"view_{artist.id}"
        )
        show_all = (view_mode == "all releases")

        # query releases for this artist (avoid lazy loads)
        with db() as s:
            rels_q = (
                s.query(Release)
                .join(ArtistRelease, ArtistRelease.release_id == Release.id)
                .filter(ArtistRelease.artist_id == artist.id)
                .filter((Release.is_variant == False) | (Release.is_variant.is_(None)))
            )
            if not show_all:
                rels_q = rels_q.filter(ArtistRelease.role == "primary")
            rels = rels_q.order_by(Release.release_date.desc().nullslast()).all()

        if not rels:
            st.write("_no releases found (try refresh after syncing)_")

        # render releases
        for r in rels:
            # count hidden variants
            with db() as s:
                variants = s.query(Release).filter(Release.variant_of == r.id).count()

            cols = st.columns([1, 5, 2.5, 2.5])
            with cols[0]:
                if r.cover:
                    st.image(r.cover, width=56)

            with cols[1]:
                year = (r.release_date or "")[:4]
                st.write(f"**{r.title}** ({year})  \n_{r.group or r.type}_")
                if variants:
                    with st.expander(f"{variants} variant(s) hidden"):
                        with db() as s:
                            vrows = s.query(Release).filter(Release.variant_of == r.id).all()
                        for v in vrows:
                            vy = (v.release_date or "")[:4]
                            st.write(f"- {v.title} ({vy})")

                # album rating badge (override or computed from tracks)
                with db() as s:
                    disp_rating = album_display_rating(s, r.id)
                rating_badge("album rating", disp_rating)

            # listened toggle (release-level)
            with db() as s:
                state = s.get(UserReleaseState, r.id)
                listened_val = state.listened if state else False
                rating_val = state.rating if (state and state.rating is not None) else 0

            with cols[2]:
                if st.checkbox("listened", value=listened_val, key=f"listened_{r.id}"):
                    with db() as s:
                        row = s.get(UserReleaseState, r.id)
                        if row is None:
                            row = UserReleaseState(release_id=r.id, listened=True)
                            s.add(row)
                        else:
                            row.listened = True
                        s.commit()
                else:
                    with db() as s:
                        row = s.get(UserReleaseState, r.id)
                        if row:
                            row.listened = False
                            s.commit()

            # album rating override slider
            with cols[3]:
                ov_new = st.slider("album rating", 0, 10, value=rating_val, key=f"rating_{r.id}")
                if ov_new != rating_val:
                    with db() as s:
                        row = s.get(UserReleaseState, r.id)
                        if row is None:
                            row = UserReleaseState(release_id=r.id, rating=int(ov_new))
                            s.add(row)
                        else:
                            row.rating = int(ov_new)
                        s.commit()

            # per-track ratings ui (compact)
            with st.expander("tracks & rating"):
                # list tracks ordered
                with db() as s:
                    tracks = (
                        s.query(Track)
                        .filter(Track.release_id == r.id)
                        .order_by(Track.disc_number.asc(), Track.track_number.asc())
                        .all()
                    )
                    # map existing states
                    t_states = {t.id: s.get(UserTrackState, t.id) for t in tracks}

                for t in tracks:
                    c1, c2, c3 = st.columns([6, 2, 2])
                    with c1:
                        st.write(f"{t.disc_number}.{t.track_number} — **{t.title}**")

                    # track listened
                    listened_key = f"tl_{t.id}"
                    listened_val = (t_states[t.id].listened if t_states[t.id] else False)
                    new_listened = st.checkbox("listened", value=listened_val, key=listened_key)
                    if new_listened != listened_val:
                        with db() as s:
                            row = s.get(UserTrackState, t.id)
                            if row is None:
                                row = UserTrackState(track_id=t.id, listened=new_listened)
                                s.add(row)
                            else:
                                row.listened = new_listened
                            s.commit()

                    # track rating
                    rating_key = f"tr_{t.id}"
                    cur_rating = (t_states[t.id].rating if t_states[t.id] and t_states[t.id].rating is not None else 0)
                    new_rating = st.select_slider(
                        "rating", options=list(range(0, 11)),
                        value=cur_rating, key=rating_key, label_visibility="collapsed"
                    )
                    if new_rating != cur_rating:
                        with db() as s:
                            row = s.get(UserTrackState, t.id)
                            if row is None:
                                row = UserTrackState(track_id=t.id, rating=int(new_rating))
                                s.add(row)
                            else:
                                row.rating = int(new_rating)
                            s.commit()

                # show computed rating from tracks after edits
                with db() as s:
                    comp = album_display_rating(s, r.id)
                rating_badge("computed from tracks", comp)

        st.markdown("---")
