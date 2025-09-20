# streamlit_app.py
# artist grid (sortable), collapsible artist detail with tabs, singles as track cards,
# bonus releases as album cards, robust duplicate propagation, and subtle css polish

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
st.set_page_config(page_title="my music list", page_icon="🎶", layout="wide")
load_dotenv(dotenv_path=Path(__file__).parent / ".env")
init_db()

CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
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

# ---------- light theme polish (css) ----------
st.markdown("""
<style>
/* center text in image captions */
.block-container { padding-top: 1.2rem; }
h1, h2, h3 { letter-spacing: 0.2px; }
img { border-radius: 12px; }
div[data-testid="stMetricDelta"] { justify-content: center; }
.card {
  border: 1px solid #e6e6e6; border-radius: 16px; padding: 10px 12px;
  box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}
.rating-badge { font-weight:600; padding:4px 8px; border-radius:10px; }
</style>
""", unsafe_allow_html=True)

# ---------- helpers: visuals + math ----------

def rating_color(score: int | float | None) -> str:
    if score is None:
        return "#666666"
    s = max(0.0, min(10.0, float(score)))
    if s <= 5.0:
        t = s / 5.0
        r, g, b = 255, int(255 * t), 0
    else:
        t = (s - 5.0) / 5.0
        r, g, b = int(255 * (1 - t)), 255, 0
    return f"#{r:02x}{g:02x}{b:02x}"

def rating_badge(label: str, score: float | None):
    col = rating_color(score)
    txt = "—" if score is None else f"{score:.1f}"
    st.markdown(
        f'<span class="rating-badge" style="background:{col}20;border:1px solid {col};color:{col};">'
        f'{label}: {txt}</span>',
        unsafe_allow_html=True
    )

def album_display_rating(s, album_id: str) -> float | None:
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

def ms_to_minsec(ms: int | None) -> str:
    if not ms:
        return "—"
    s = int(ms // 1000)
    return f"{s//60}:{s%60:02d}"

# duplicate propagation: mirror listened/rating to all duplicates and refresh UI
def propagate_track_state(track_id: str, artist_id: str, listened: bool | None = None, rating: int | None = None):
    with db() as s:
        t = s.get(Track, track_id)
        if not t:
            return
        matches = []
        if t.canonical_key:
            matches = s.query(Track).filter(Track.canonical_key == t.canonical_key).all()
        elif t.isrc:
            matches = s.query(Track).filter(Track.isrc == t.isrc).all()
        else:
            return
        for m in matches:
            ids = (m.artist_ids_csv or "").split(",") if m.artist_ids_csv else []
            # ensure relevant to target artist (overlap)
            if (artist_id not in ids) and ((m.primary_artist_id or "") != artist_id):
                continue
            row = s.get(UserTrackState, m.id) or UserTrackState(track_id=m.id)
            if listened is not None:
                row.listened = bool(listened)
            if rating is not None:
                row.rating = int(rating)
            s.add(row)
        s.commit()
    st.rerun()

# compact track card used for singles and features
def track_card(track, artist_id: str):
    names = (track.artist_names_csv or "").split(",") if track.artist_names_csv else []
    ids = (track.artist_ids_csv or "").split(",") if track.artist_ids_csv else []
    featured = [n for (i, n) in zip(ids, names) if i and i != artist_id]
    feat_txt = f" • feat. {', '.join(featured)}" if featured else ""
    duration = ms_to_minsec(track.duration_ms)

    c1, c2, c3 = st.columns([6, 2, 2])
    with c1:
        st.write(f"**{track.title}**{feat_txt}")
        st.caption(duration)

    with db() as s:
        ts = s.get(UserTrackState, track.id)
        listened_val = ts.listened if ts else False
        rating_val = ts.rating if (ts and ts.rating is not None) else 0

    with c2:
        new_listened = st.checkbox("listened", value=listened_val, key=f"tl_{track.id}")
        if new_listened != listened_val:
            propagate_track_state(track.id, artist_id, listened=new_listened)

    with c3:
        new = st.select_slider("rating", options=list(range(0, 11)),
                               value=rating_val, key=f"tr_{track.id}", label_visibility="collapsed")
        if new != rating_val:
            propagate_track_state(track.id, artist_id, rating=int(new))

# -------- artist grid helpers --------
def artist_metrics():
    with db() as s:
        artists = s.query(Artist).all()
        out = []
        for a in artists:
            p_listened, p_total, p_pct = artist_completion(s, a.id, primary_only=True)
            out.append({
                "id": a.id,
                "name": a.name,
                "image": a.image,
                "pct": p_pct,
                "count": (p_listened, p_total),
            })
        return out

def render_artist_grid(artists, sort_by="pct", cols=5):
    if sort_by == "name":
        artists = sorted(artists, key=lambda x: x["name"].lower())
    elif sort_by == "pct":
        artists = sorted(artists, key=lambda x: (-x["pct"], x["name"].lower()))
    rows = (len(artists) + cols - 1) // cols
    open_set = st.session_state.setdefault("open_artist", set())
    for r in range(rows):
        cs = st.columns(cols, gap="small")
        for i in range(cols):
            idx = r*cols + i
            if idx >= len(artists): break
            a = artists[idx]
            with cs[i]:
                st.markdown('<div class="card">', unsafe_allow_html=True)
                if a["image"]:
                    st.image(a["image"], width=220)
                st.markdown(
                    f"<div style='text-align:center;font-weight:700;margin-top:6px'>{a['name']}</div>",
                    unsafe_allow_html=True
                )
                st.progress(int(a["pct"]))
                st.caption(f"{a['count'][0]}/{a['count'][1]} • {a['pct']:.0f}%")
                if st.button("open", key=f"open_{a['id']}"):
                    open_set.add(a["id"])
                    st.session_state["open_artist"] = open_set
                    st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

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

# artist grid (sortable)
st.subheader("your artists")
sort_choice = st.radio("sort by", ["% complete", "name"], horizontal=True)
sort_key = "pct" if sort_choice == "% complete" else "name"
grid_data = artist_metrics()
render_artist_grid(grid_data, sort_by=sort_key, cols=5)

st.divider()

# detail sections (collapsible by tile "open" button)
open_set = st.session_state.get("open_artist", set())
with db() as s:
    artists = s.query(Artist).order_by(Artist.name.asc()).all()

if not artists:
    st.info("no artists yet — search above to add and sync.")
else:
    for artist in artists:
        expanded_flag = (artist.id in open_set)
        with st.expander(f"open {artist.name}'s discography", expanded=expanded_flag):
            # completion meters
            with db() as s2:
                p_listened, p_total, p_pct = artist_completion(s2, artist.id, primary_only=True)
                a_listened, a_total, a_pct = artist_completion(s2, artist.id, primary_only=False)

            c1, c2, c3 = st.columns([2, 2, 1])
            with c1:
                st.write("**primary discography**")
                st.progress(int(p_pct))
                st.caption(f"{p_listened}/{p_total} • {p_pct:.0f}%")
            with c2:
                st.write("**all releases (incl. appears_on)**")
                st.progress(int(a_pct))
                st.caption(f"{a_listened}/{a_total} • {a_pct:.0f}%")
            with c3:
                if st.button("refresh", key=f"refresh_{artist.id}"):
                    with db() as s3:
                        ingest_artist(s3, sp, artist.id)
                    st.success("refreshed")

            # view toggle
            view_mode = st.radio(
                "view",
                ["primary only", "all releases"],
                horizontal=True,
                key=f"view_{artist.id}"
            )
            show_all = (view_mode == "all releases")

            # releases query
            with db() as s4:
                rels_q = (
                    s4.query(Release)
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
                # tracks (used by tabs)
                with db() as s5:
                    tracks = (
                        s5.query(Track)
                        .filter(Track.release_id == r.id)
                        .order_by(Track.disc_number.asc(), Track.track_number.asc())
                        .all()
                    )

                is_single_track = (r.group == "single" and len(tracks) == 1)

                if is_single_track:
                    cols = st.columns([1, 9])
                    with cols[0]:
                        if r.cover:
                            st.image(r.cover, width=56)
                    with cols[1]:
                        track_card(tracks[0], artist.id)
                else:
                    # album/ep/compilation row with tabs
                    st.markdown("---")
                    cols = st.columns([1, 11])
                    with cols[0]:
                        if r.cover:
                            st.image(r.cover, width=56)

                    with cols[1]:
                        tab_overview, tab_tracks = st.tabs(["Overview", "Tracks"])

                        with tab_overview:
                            year = (r.release_date or "")[:4]
                            st.write(f"**{r.title}** ({year})  \n_{r.group or r.type}_")

                            # hidden variants toggle
                            with db() as s6:
                                variants = s6.query(Release).filter(Release.variant_of == r.id).count()
                            if variants:
                                show_vars = st.checkbox(f"show {variants} variant(s)", key=f"variants_toggle_{r.id}")
                                if show_vars:
                                    with db() as s7:
                                        vrows = s7.query(Release).filter(Release.variant_of == r.id).all()
                                    for v in vrows:
                                        vy = (v.release_date or "")[:4]
                                        st.write(f"- {v.title} ({vy})")

                            # rating badge (override or computed from tracks)
                            with db() as s8:
                                disp_rating = album_display_rating(s8, r.id)
                            rating_badge("album rating", disp_rating)

                            # release-level listened and album rating override
                            with db() as s9:
                                state = s9.get(UserReleaseState, r.id)
                                listened_val = state.listened if state else False
                                rating_val = state.rating if (state and state.rating is not None) else 0

                            cA, cB = st.columns(2)
                            with cA:
                                if st.checkbox("listened", value=listened_val, key=f"listened_{r.id}"):
                                    with db() as s10:
                                        row = s10.get(UserReleaseState, r.id)
                                        if row is None:
                                            row = UserReleaseState(release_id=r.id, listened=True)
                                            s10.add(row)
                                        else:
                                            row.listened = True
                                        s10.commit()
                                else:
                                    with db() as s11:
                                        row = s11.get(UserReleaseState, r.id)
                                        if row:
                                            row.listened = False
                                            s11.commit()
                            with cB:
                                ov_new = st.slider("album rating", 0, 10, value=rating_val, key=f"rating_{r.id}")
                                if ov_new != rating_val:
                                    with db() as s12:
                                        row = s12.get(UserReleaseState, r.id)
                                        if row is None:
                                            row = UserReleaseState(release_id=r.id, rating=int(ov_new))
                                            s12.add(row)
                                        else:
                                            row.rating = int(ov_new)
                                        s12.commit()

                        with tab_tracks:
                            for t in tracks:
                                track_card(t, artist.id)

            # bonus releases: show albums they feature on, not whole album list above
            st.markdown("#### bonus releases (features on other albums)")
            with db() as sF:
                appears_album_ids = [
                    rid for (rid,) in sF.query(ArtistRelease.release_id)
                    .filter(ArtistRelease.artist_id == artist.id, ArtistRelease.role == "appears_on")
                    .all()
                ]
                if not appears_album_ids:
                    st.caption("_no features found yet_")
                else:
                    for rid in appears_album_ids:
                        album = sF.get(Release, rid)
                        album_tracks = (
                            sF.query(Track)
                            .filter(Track.release_id == rid)
                            .all()
                        )
                        feat_tracks = [
                            t for t in album_tracks
                            if artist.id in ((t.artist_ids_csv or "").split(",") if t.artist_ids_csv else [])
                        ]
                        if not feat_tracks:
                            continue
                        cols = st.columns([1, 11])
                        with cols[0]:
                            if album.cover:
                                st.image(album.cover, width=56)
                        with cols[1]:
                            tab_overview, tab_tracks = st.tabs(["Overview", "Tracks"])
                            with tab_overview:
                                year = (album.release_date or "")[:4]
                                st.write(f"**{album.title}** ({year})  _bonus release_")
                                st.caption(f"features {len(feat_tracks)} track(s)")
                            with tab_tracks:
                                for t in sorted(feat_tracks, key=lambda x: (x.disc_number or 1, x.track_number or 1)):
                                    track_card(t, artist.id)
