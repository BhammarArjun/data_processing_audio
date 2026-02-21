from __future__ import annotations

import json
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any


def _safe_track_key(value: str) -> str:
    key = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return key.strip("_") or "track"


def _load_entries(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        return []
    entries: list[dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        text = str(entry.get("text", "")).strip()
        start = float(entry.get("start", 0.0))
        duration = float(entry.get("duration", 0.0))
        if duration < 0:
            duration = 0.0
        entries.append({"text": text, "start": start, "duration": duration})
    return entries


def _collect_text_in_window(
    entries: list[dict[str, Any]], start: float, end: float
) -> tuple[str, list[int]]:
    texts: list[str] = []
    matched_indices: list[int] = []
    for idx, entry in enumerate(entries):
        entry_start = float(entry["start"])
        entry_end = entry_start + float(entry["duration"])
        if entry_end <= start or entry_start >= end:
            continue
        text = str(entry["text"]).strip()
        if not text:
            continue
        texts.append(text)
        matched_indices.append(idx)
    return " ".join(texts).strip(), matched_indices


def _codec_args(audio_format: str, bitrate: str) -> list[str]:
    fmt = audio_format.lower()
    if fmt == "mp3":
        return ["-c:a", "libmp3lame", "-b:a", bitrate]
    if fmt in {"wav", "wave"}:
        return ["-c:a", "pcm_s16le"]
    if fmt in {"m4a", "aac"}:
        return ["-c:a", "aac", "-b:a", bitrate]
    if fmt == "flac":
        return ["-c:a", "flac"]
    if fmt == "opus":
        return ["-c:a", "libopus", "-b:a", bitrate]
    return []


def _run_ffmpeg_cut(
    source_audio: Path,
    output_audio: Path,
    *,
    start: float,
    duration: float,
    audio_format: str,
    bitrate: str,
    ffmpeg_bin: str,
) -> None:
    output_audio.parent.mkdir(parents=True, exist_ok=True)
    command = [
        ffmpeg_bin,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-threads",
        "1",
        "-ss",
        f"{start:.3f}",
        "-t",
        f"{duration:.3f}",
        "-i",
        str(source_audio),
        "-vn",
        *_codec_args(audio_format, bitrate),
        str(output_audio),
    ]
    subprocess.run(command, check=True)


def _collect_transcript_tracks(transcript_summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    tracks: dict[str, dict[str, Any]] = {}

    def add_track(key: str, path: str | None, *, language_code: str | None, is_generated: bool | None) -> None:
        if not path:
            return
        track_path = Path(path)
        if not track_path.exists():
            return
        unique_key = _safe_track_key(key)
        counter = 2
        while unique_key in tracks:
            unique_key = f"{_safe_track_key(key)}_{counter}"
            counter += 1
        tracks[unique_key] = {
            "path": track_path,
            "language_code": language_code,
            "is_generated": is_generated,
        }

    add_track("default", transcript_summary.get("default_path"), language_code=None, is_generated=None)
    add_track(
        f"auto_target_{transcript_summary.get('auto_language_code') or 'unknown'}",
        transcript_summary.get("auto_language_path"),
        language_code=transcript_summary.get("auto_language_code"),
        is_generated=True,
    )

    for item in transcript_summary.get("available", []):
        if not isinstance(item, dict):
            continue
        code = str(item.get("language_code", "unknown"))
        kind = "auto" if item.get("is_generated") else "manual"
        add_track(
            f"{kind}_{code}",
            item.get("path"),
            language_code=code,
            is_generated=bool(item.get("is_generated")),
        )

    return tracks


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def create_transcript_aligned_segments(
    *,
    source_audio_path: Path,
    transcript_summary: dict[str, Any],
    output_root: Path,
    overwrite: bool = False,
    min_duration: float = 0.25,
    min_chars: int = 1,
    segment_audio_format: str = "mp3",
    segment_audio_bitrate: str = "128k",
    workers: int = 1,
    ffmpeg_bin: str = "ffmpeg",
) -> dict[str, Any]:
    """
    Create transcript-timed cuts and store per-segment transcript bundles.

    Layout:
    - output_root/index.jsonl
    - output_root/<segment_id>/audio.<format>
    - output_root/<segment_id>/transcripts.json
    """
    tracks = _collect_transcript_tracks(transcript_summary)
    if not tracks:
        return {
            "segment_count": 0,
            "skipped_count": 0,
            "base_track": None,
            "index_path": None,
            "segments_dir": str(output_root),
            "error": "No transcript files available for segmentation.",
        }

    output_root.mkdir(parents=True, exist_ok=True)
    entries_by_track = {track_key: _load_entries(track_data["path"]) for track_key, track_data in tracks.items()}

    base_track = "default" if "default" in entries_by_track else next(iter(entries_by_track.keys()))
    base_entries = entries_by_track[base_track]

    if workers < 1:
        raise ValueError("workers must be >= 1")

    prepared_segments: list[dict[str, Any]] = []
    skipped_count = 0
    kept_index = 0
    for base_entry_index, entry in enumerate(base_entries):
        text = str(entry["text"]).strip()
        start = max(0.0, float(entry["start"]))
        duration = float(entry["duration"])
        if duration < min_duration or len(text) < min_chars:
            skipped_count += 1
            continue

        segment_id = f"{kept_index:06d}"
        kept_index += 1
        end = start + duration
        segment_dir = output_root / segment_id
        audio_path = segment_dir / f"audio.{segment_audio_format}"
        transcript_bundle_path = segment_dir / "transcripts.json"

        tracks_bundle: dict[str, Any] = {}
        for track_key, track_entries in entries_by_track.items():
            track_text, matched_indices = _collect_text_in_window(track_entries, start, end)
            track_meta = tracks[track_key]
            tracks_bundle[track_key] = {
                "text": track_text,
                "entry_indices": matched_indices,
                "language_code": track_meta["language_code"],
                "is_generated": track_meta["is_generated"],
            }

        bundle = {
            "segment_id": segment_id,
            "timing": {
                "start": start,
                "duration": duration,
                "end": end,
                "base_track": base_track,
                "base_entry_index": base_entry_index,
            },
            "tracks": tracks_bundle,
        }
        prepared_segments.append(
            {
                "start": start,
                "duration": duration,
                "audio_path": audio_path,
                "transcript_bundle_path": transcript_bundle_path,
                "bundle": bundle,
                "row": {
                    "segment_id": segment_id,
                    "start": start,
                    "duration": duration,
                    "end": end,
                    "base_track": base_track,
                    "audio_path": str(audio_path),
                    "transcripts_path": str(transcript_bundle_path),
                    "base_text": text,
                },
            }
        )

    def materialize_segment(item: dict[str, Any]) -> None:
        audio_path = item["audio_path"]
        transcript_bundle_path = item["transcript_bundle_path"]
        if overwrite or not audio_path.exists():
            _run_ffmpeg_cut(
                source_audio_path,
                audio_path,
                start=float(item["start"]),
                duration=float(item["duration"]),
                audio_format=segment_audio_format,
                bitrate=segment_audio_bitrate,
                ffmpeg_bin=ffmpeg_bin,
            )
        transcript_bundle_path.parent.mkdir(parents=True, exist_ok=True)
        transcript_bundle_path.write_text(json.dumps(item["bundle"], ensure_ascii=False, indent=2), encoding="utf-8")

    if workers == 1:
        for item in prepared_segments:
            materialize_segment(item)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(materialize_segment, item) for item in prepared_segments]
            for future in as_completed(futures):
                future.result()

    index_path = output_root / "index.jsonl"
    rows = [item["row"] for item in prepared_segments]
    _write_jsonl(index_path, rows)
    return {
        "segment_count": len(rows),
        "skipped_count": skipped_count,
        "base_track": base_track,
        "index_path": str(index_path),
        "segments_dir": str(output_root),
        "error": None,
    }
