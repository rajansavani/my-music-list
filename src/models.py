# all comments in lowercase per your style
from sqlalchemy import (
    Column, String, Integer, Boolean, Text, ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import relationship
from .db import Base

# enums as simple strings to avoid migrations pain early
RELEASE_TYPE = ("album", "ep", "single", "compilation", "mixtape", "unreleased")
RELEASE_GROUP = ("album", "single", "compilation", "appears_on")  # spotify "album_group"

class Artist(Base):
    __tablename__ = "artists"
    id = Column(String, primary_key=True)          # spotify artist id or manual_xxx
    name = Column(String, nullable=False)
    image = Column(Text)

    # relationships
    releases = relationship("ArtistRelease", back_populates="artist", cascade="all, delete-orphan")

class Release(Base):
    __tablename__ = "releases"
    id = Column(String, primary_key=True)          # spotify album id or manual_xxx
    title = Column(String, nullable=False)
    type = Column(String)                          # one of RELEASE_TYPE
    group = Column(String)                         # one of RELEASE_GROUP (spotify album_group)
    release_date = Column(String)                  # raw string (yyyy or yyyy-mm or yyyy-mm-dd)
    date_precision = Column(String)                # 'year' | 'month' | 'day'
    total_tracks = Column(Integer)
    cover = Column(Text)
    source = Column(String, default="spotify")     # 'spotify' | 'manual'

    # variant handling: link deluxe/clean/regional variants to a canonical release
    variant_of = Column(String, ForeignKey("releases.id"), nullable=True)
    is_variant = Column(Boolean, default=False)

    # optional ids for better dedupe (when available)
    upc = Column(String)                           # spotify sometimes exposes upc on album lookup

    # relationships
    artists = relationship("ArtistRelease", back_populates="release", cascade="all, delete-orphan")
    tracks = relationship("Track", back_populates="release", cascade="all, delete-orphan")
    canonical = relationship("Release", remote_side=[id])

    __table_args__ = (
        UniqueConstraint("id", "source", name="uq_release_id_source"),
        Index("ix_release_title", "title"),
    )

class ArtistRelease(Base):
    __tablename__ = "artist_releases"
    artist_id = Column(String, ForeignKey("artists.id"), primary_key=True)
    release_id = Column(String, ForeignKey("releases.id"), primary_key=True)
    role = Column(String, default="primary")       # 'primary' | 'appears_on'

    artist = relationship("Artist", back_populates="releases")
    release = relationship("Release", back_populates="artists")

class Track(Base):
    __tablename__ = "tracks"
    id = Column(String, primary_key=True)          # spotify track id or manual_xxx
    release_id = Column(String, ForeignKey("releases.id"), index=True, nullable=False)
    title = Column(String, nullable=False)
    disc_number = Column(Integer, default=1)
    track_number = Column(Integer, default=1)
    duration_ms = Column(Integer)
    explicit = Column(Boolean, default=False)
    isrc = Column(String)                          # key for cross-release dedupe when available

    # store artists on the track so we can detect features
    artist_ids_csv = Column(Text)                  # e.g., "id1,id2"
    artist_names_csv = Column(Text)                # e.g., "name1,name2"

    # duplicate detection heuristic (fallback when no isrc):
    # normalized title + duration (sec bucket) + sorted artist ids
    canonical_key = Column(String, index=True)
    primary_artist_id = Column(String, index=True) # first artist id on the track if present

    release = relationship("Release", back_populates="tracks")

    __table_args__ = (
        Index("ix_track_isrc", "isrc"),
        Index("ix_track_canon", "canonical_key"),
    )

# user state at release-level
class UserReleaseState(Base):
    __tablename__ = "user_release_state"
    release_id = Column(String, ForeignKey("releases.id"), primary_key=True)
    listened = Column(Boolean, default=False)
    rating = Column(Integer)                       # 0..10
    notes = Column(Text)

    release = relationship("Release")

# user state at track-level
class UserTrackState(Base):
    __tablename__ = "user_track_state"
    track_id = Column(String, ForeignKey("tracks.id"), primary_key=True)
    listened = Column(Boolean, default=False)
    rating = Column(Integer)
