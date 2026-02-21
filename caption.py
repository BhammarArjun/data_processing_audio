from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import NoTranscriptFound


def _write_transcript_file(path: Path, raw_entries: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    entries = [
        {
            "text": entry.get("text", ""),
            "start": float(entry.get("start", 0.0)),
            "duration": float(entry.get("duration", 0.0)),
        }
        for entry in raw_entries
    ]
    path.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")


def _first_available_transcript(transcripts: list[Any]) -> Any | None:
    manual = [transcript for transcript in transcripts if not transcript.is_generated]
    return manual[0] if manual else (transcripts[0] if transcripts else None)


def _resolve_target_auto_transcript(transcript_list: Any, language_code: str) -> tuple[Any | None, str]:
    try:
        return transcript_list.find_generated_transcript([language_code]), "generated"
    except NoTranscriptFound:
        pass

    try:
        return transcript_list.find_transcript([language_code]), "direct"
    except NoTranscriptFound:
        pass

    for transcript in transcript_list:
        if transcript.is_translatable:
            return transcript.translate(language_code), "translated"

    return None, "missing"


def _resolve_detected_auto_transcript(transcripts: list[Any]) -> tuple[Any | None, str, str | None]:
    for transcript in transcripts:
        if transcript.is_generated:
            return transcript, "detected_generated", transcript.language_code
    return None, "missing", None


def fetch_and_store_transcripts(
    video_id: str,
    transcripts_root: Path,
    *,
    auto_language: str | None = None,
    include_all_transcripts: bool = True,
    overwrite: bool = False,
) -> dict[str, Any]:
    """
    Save transcripts under transcripts_root and return a summary.

    Directory layout:
    - default.json
    - auto_<language_code>.json (optional)
    - manual/<language_code>.json (optional)
    - auto/<language_code>.json (optional)
    """
    transcripts_root.mkdir(parents=True, exist_ok=True)
    ytt_api = YouTubeTranscriptApi()
    transcript_list = ytt_api.list(video_id)
    all_transcripts = list(transcript_list)

    summary: dict[str, Any] = {
        "default_path": None,
        "auto_language_path": None,
        "auto_language_mode": None,
        "auto_language_code": None,
        "available": [],
    }

    default_transcript = _first_available_transcript(all_transcripts)
    default_path = transcripts_root / "default.json"
    if default_transcript:
        if overwrite or not default_path.exists():
            _write_transcript_file(default_path, default_transcript.fetch().to_raw_data())
        summary["default_path"] = str(default_path)

    if auto_language:
        target_transcript, mode = _resolve_target_auto_transcript(transcript_list, auto_language)
        if target_transcript:
            target_path = transcripts_root / f"auto_{auto_language}.json"
            if overwrite or not target_path.exists():
                _write_transcript_file(target_path, target_transcript.fetch().to_raw_data())
            summary["auto_language_path"] = str(target_path)
            summary["auto_language_mode"] = mode
            summary["auto_language_code"] = auto_language
        else:
            summary["auto_language_mode"] = "missing"
            summary["auto_language_code"] = auto_language
    else:
        detected_transcript, mode, code = _resolve_detected_auto_transcript(all_transcripts)
        if detected_transcript and code:
            target_path = transcripts_root / f"auto_detected_{code}.json"
            if overwrite or not target_path.exists():
                _write_transcript_file(target_path, detected_transcript.fetch().to_raw_data())
            summary["auto_language_path"] = str(target_path)
            summary["auto_language_mode"] = mode
            summary["auto_language_code"] = code
        else:
            summary["auto_language_mode"] = "missing"

    if include_all_transcripts:
        for transcript in all_transcripts:
            kind = "auto" if transcript.is_generated else "manual"
            filename = f"{transcript.language_code}.json"
            path = transcripts_root / kind / filename
            if overwrite or not path.exists():
                _write_transcript_file(path, transcript.fetch().to_raw_data())

            summary["available"].append(
                {
                    "language": transcript.language,
                    "language_code": transcript.language_code,
                    "is_generated": bool(transcript.is_generated),
                    "path": str(path),
                }
            )

    return summary
