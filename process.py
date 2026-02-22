from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from audio import download_audio, fetch_video_info
from caption import fetch_and_store_transcripts
from segment import create_transcript_aligned_segments


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch YouTube dataset pipeline for ASR/TTS (audio + captions)."
    )
    parser.add_argument(
        "--urls-file",
        required=True,
        help="Path to URLs file (.txt with one URL per line, or .json with an array).",
    )
    parser.add_argument(
        "--dataset-root",
        default="dataset",
        help="Output dataset root directory. Default: dataset",
    )
    parser.add_argument(
        "--system",
        default="auto",
        choices=["auto", "mac", "linux"],
        help="Target runtime system for tuning defaults. Default: auto",
    )
    parser.add_argument(
        "--auto-language",
        default=None,
        help="Optional language code for target caption export. If omitted, auto-generated caption language is detected automatically.",
    )
    parser.add_argument(
        "--cookies",
        default=None,
        help="Path to Netscape-format cookies.txt for YouTube auth (recommended on Linux).",
    )
    parser.add_argument(
        "--cookies-from-browser",
        default=None,
        help="Browser cookie source in format BROWSER[+KEYRING][:PROFILE][::CONTAINER], e.g. firefox:default-release",
    )
    parser.add_argument(
        "--audio-format",
        default="mp3",
        help="Audio codec extension for FFmpegExtractAudio. Default: mp3",
    )
    parser.add_argument(
        "--audio-quality",
        default="192",
        help="Audio quality for FFmpegExtractAudio. Default: 192",
    )
    parser.add_argument(
        "--skip-all-transcripts",
        action="store_true",
        help="Only save default and auto-language transcripts, skip manual/auto folder dump.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-download and overwrite existing files for processed videos.",
    )
    parser.add_argument(
        "--video-workers",
        type=int,
        default=0,
        help="Parallel video workers. 0 means auto (use all CPU cores).",
    )
    parser.add_argument(
        "--no-segments",
        action="store_true",
        help="Skip transcript-timed audio segment generation.",
    )
    parser.add_argument(
        "--segment-workers",
        type=int,
        default=0,
        help="Parallel workers per video for transcript-timed cuts. 0 means auto.",
    )
    parser.add_argument(
        "--ffmpeg-bin",
        default="ffmpeg",
        help="ffmpeg binary path/name. Default: ffmpeg",
    )
    parser.add_argument(
        "--segment-format",
        default="mp3",
        help="Audio format for transcript-timed segments. Default: mp3",
    )
    parser.add_argument(
        "--segment-bitrate",
        default="128k",
        help="Bitrate for compressed segment formats. Default: 128k",
    )
    parser.add_argument(
        "--segment-min-duration",
        type=float,
        default=0.25,
        help="Minimum duration (seconds) required to keep a segment. Default: 0.25",
    )
    parser.add_argument(
        "--segment-min-chars",
        type=int,
        default=1,
        help="Minimum text length required to keep a segment. Default: 1",
    )
    return parser.parse_args()


def load_urls(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"URLs file not found: {path}")

    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            urls = [str(item).strip() for item in data]
        elif isinstance(data, dict) and isinstance(data.get("urls"), list):
            urls = [str(item).strip() for item in data["urls"]]
        else:
            raise ValueError("JSON URLs file must be a list or {\"urls\": [...]} object.")
    else:
        lines = path.read_text(encoding="utf-8").splitlines()
        urls = [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]

    unique_urls: list[str] = []
    seen = set()
    for url in urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)
    return unique_urls


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def detect_current_system() -> str:
    current = platform.system().lower()
    if current == "darwin":
        return "mac"
    if current == "linux":
        return "linux"
    return "linux"


def resolve_runtime(
    *,
    system_arg: str,
    video_workers_arg: int,
    segment_workers_arg: int,
) -> dict[str, Any]:
    cpu_count = os.cpu_count() or 1
    detected_system = detect_current_system()
    selected_system = detected_system if system_arg == "auto" else system_arg

    if video_workers_arg < 0 or segment_workers_arg < 0:
        raise ValueError("Worker counts must be >= 0.")

    video_workers = video_workers_arg if video_workers_arg > 0 else max(1, cpu_count)
    if segment_workers_arg > 0:
        segment_workers = segment_workers_arg
    else:
        # Keep total ffmpeg concurrency near CPU count by default.
        segment_workers = max(1, cpu_count // max(1, video_workers))

    return {
        "cpu_count": cpu_count,
        "detected_system": detected_system,
        "system": selected_system,
        "video_workers": video_workers,
        "segment_workers": segment_workers,
    }


def parse_cookies_from_browser(value: str | None) -> tuple[str, str | None, str | None, str | None] | None:
    if not value:
        return None
    match = re.fullmatch(
        r"(?x)(?P<name>[^+:]+)(?:\s*\+\s*(?P<keyring>[^:]+))?(?:\s*:\s*(?!:)(?P<profile>.+?))?(?:\s*::\s*(?P<container>.+))?",
        value,
    )
    if match is None:
        raise ValueError(f"Invalid --cookies-from-browser format: {value}")

    browser_name, keyring, profile, container = match.group("name", "keyring", "profile", "container")
    browser_name = browser_name.lower()
    keyring = keyring.upper() if keyring else None
    return browser_name, profile, keyring, container


def resolve_cookie_file(path_value: str | None) -> str | None:
    if not path_value:
        return None
    cookie_path = Path(path_value).expanduser().resolve()
    if not cookie_path.exists():
        raise FileNotFoundError(f"Cookie file not found: {cookie_path}")
    return str(cookie_path)


def to_relative(path: str | Path | None, root: Path) -> str | None:
    if path is None:
        return None
    return str(Path(path).resolve().relative_to(root.resolve()))


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def process_urls_batch(
    urls: list[str],
    dataset_root: Path,
    *,
    auto_language: str | None,
    cookie_file: str | None,
    cookies_from_browser: tuple[str, str | None, str | None, str | None] | None,
    audio_format: str,
    audio_quality: str,
    include_all_transcripts: bool,
    overwrite: bool,
    generate_segments: bool,
    segment_format: str,
    segment_bitrate: str,
    segment_min_duration: float,
    segment_min_chars: int,
    segment_workers: int,
    ffmpeg_bin: str,
    video_workers: int,
    label: str = "video",
) -> list[dict[str, Any]]:
    if not urls:
        return []

    records: list[dict[str, Any] | None] = [None] * len(urls)

    def status_suffix(record: dict[str, Any]) -> str:
        status = str(record.get("status", "unknown"))
        error = str(record.get("error") or "").strip()
        if status == "success" or not error:
            return status
        compact_error = " ".join(error.split())
        if len(compact_error) > 180:
            compact_error = compact_error[:177] + "..."
        return f"{status} ({compact_error})"

    def run_single(idx: int, url: str) -> tuple[int, dict[str, Any]]:
        try:
            record = process_url(
                url,
                dataset_root,
                auto_language=auto_language,
                cookie_file=cookie_file,
                cookies_from_browser=cookies_from_browser,
                audio_format=audio_format,
                audio_quality=audio_quality,
                include_all_transcripts=include_all_transcripts,
                overwrite=overwrite,
                generate_segments=generate_segments,
                segment_format=segment_format,
                segment_bitrate=segment_bitrate,
                segment_min_duration=segment_min_duration,
                segment_min_chars=segment_min_chars,
                segment_workers=segment_workers,
                ffmpeg_bin=ffmpeg_bin,
            )
        except Exception as exc:  # noqa: BLE001
            record = {
                "url": url,
                "status": "failed",
                "error": str(exc),
                "started_at": None,
                "finished_at": now_iso(),
            }
        return idx, record

    if video_workers <= 1:
        for index, url in enumerate(urls):
            idx, record = run_single(index, url)
            records[idx] = record
            print(f"[{label} {idx + 1}/{len(urls)}] {url} -> {status_suffix(record)}")
    else:
        with ThreadPoolExecutor(max_workers=video_workers) as executor:
            futures = {executor.submit(run_single, idx, url): (idx, url) for idx, url in enumerate(urls)}
            completed = 0
            for future in as_completed(futures):
                idx, url = futures[future]
                completed += 1
                out_idx, record = future.result()
                records[out_idx] = record
                print(f"[{label} {completed}/{len(urls)}] {url} -> {status_suffix(record)}")

    return [record for record in records if record is not None]


def process_url(
    url: str,
    dataset_root: Path,
    *,
    auto_language: str | None,
    cookie_file: str | None,
    cookies_from_browser: tuple[str, str | None, str | None, str | None] | None,
    audio_format: str,
    audio_quality: str,
    include_all_transcripts: bool,
    overwrite: bool,
    generate_segments: bool,
    segment_format: str,
    segment_bitrate: str,
    segment_min_duration: float,
    segment_min_chars: int,
    segment_workers: int,
    ffmpeg_bin: str,
) -> dict[str, Any]:
    started_at = now_iso()
    record: dict[str, Any] = {
        "url": url,
        "status": "failed",
        "started_at": started_at,
    }

    info = fetch_video_info(
        url,
        cookie_file=cookie_file,
        cookies_from_browser=cookies_from_browser,
    )
    video_id = info["id"]
    video_root = dataset_root / "videos" / video_id
    audio_dir = video_root / "audio"
    transcripts_dir = video_root / "transcripts"
    video_root.mkdir(parents=True, exist_ok=True)

    audio_path = download_audio(
        url,
        audio_dir,
        cookie_file=cookie_file,
        cookies_from_browser=cookies_from_browser,
        audio_format=audio_format,
        audio_quality=audio_quality,
        overwrite=overwrite,
    )

    transcript_error = None
    transcript_summary: dict[str, Any] = {
        "default_path": None,
        "auto_language_path": None,
        "auto_language_mode": "missing",
        "auto_language_code": None,
        "available": [],
    }
    try:
        transcript_summary = fetch_and_store_transcripts(
            video_id,
            transcripts_dir,
            auto_language=auto_language,
            include_all_transcripts=include_all_transcripts,
            overwrite=overwrite,
        )
    except Exception as exc:  # noqa: BLE001
        transcript_error = str(exc)

    segment_error = None
    segment_summary: dict[str, Any] = {
        "segment_count": 0,
        "skipped_count": 0,
        "base_track": None,
        "index_path": None,
        "segments_dir": None,
        "error": None,
    }
    if generate_segments and transcript_error is None:
        try:
            segment_summary = create_transcript_aligned_segments(
                source_audio_path=audio_path,
                transcript_summary=transcript_summary,
                output_root=video_root / "segments",
                overwrite=overwrite,
                min_duration=segment_min_duration,
                min_chars=segment_min_chars,
                segment_audio_format=segment_format,
                segment_audio_bitrate=segment_bitrate,
                workers=segment_workers,
                ffmpeg_bin=ffmpeg_bin,
            )
            segment_error = segment_summary.get("error")
        except Exception as exc:  # noqa: BLE001
            segment_error = str(exc)
    elif generate_segments and transcript_error is not None:
        segment_error = "Skipped because transcript fetch failed."

    metadata = {
        "video_id": video_id,
        "url": url,
        "title": info.get("title"),
        "channel": info.get("channel"),
        "uploader": info.get("uploader"),
        "duration_seconds": info.get("duration"),
        "upload_date": info.get("upload_date"),
        "language_hint": info.get("language"),
        "audio_path": to_relative(audio_path, dataset_root),
        "transcripts": {
            "default_path": to_relative(transcript_summary.get("default_path"), dataset_root),
            "auto_language": auto_language or transcript_summary.get("auto_language_code"),
            "auto_language_mode": transcript_summary.get("auto_language_mode"),
            "auto_language_path": to_relative(transcript_summary.get("auto_language_path"), dataset_root),
            "available": [
                {
                    **item,
                    "path": to_relative(item.get("path"), dataset_root),
                }
                for item in transcript_summary.get("available", [])
            ],
            "error": transcript_error,
        },
        "segments": {
            "enabled": bool(generate_segments),
            "segment_count": int(segment_summary.get("segment_count", 0)),
            "skipped_count": int(segment_summary.get("skipped_count", 0)),
            "base_track": segment_summary.get("base_track"),
            "segment_format": segment_format if generate_segments else None,
            "segment_workers": segment_workers if generate_segments else None,
            "ffmpeg_bin": ffmpeg_bin if generate_segments else None,
            "index_path": to_relative(segment_summary.get("index_path"), dataset_root),
            "segments_dir": to_relative(segment_summary.get("segments_dir"), dataset_root),
            "error": segment_error,
        },
        "auth": {
            "cookie_file_provided": bool(cookie_file),
            "cookies_from_browser_provided": bool(cookies_from_browser),
        },
        "created_at": now_iso(),
    }
    metadata_path = video_root / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    record.update(
        {
            "status": "success" if transcript_error is None and segment_error is None else "partial",
            "video_id": video_id,
            "title": info.get("title"),
            "duration_seconds": info.get("duration"),
            "audio_path": to_relative(audio_path, dataset_root),
            "default_transcript_path": to_relative(transcript_summary.get("default_path"), dataset_root),
            "auto_language": auto_language or transcript_summary.get("auto_language_code"),
            "auto_transcript_path": to_relative(transcript_summary.get("auto_language_path"), dataset_root),
            "auto_transcript_mode": transcript_summary.get("auto_language_mode"),
            "segment_count": int(segment_summary.get("segment_count", 0)),
            "segments_index_path": to_relative(segment_summary.get("index_path"), dataset_root),
            "metadata_path": to_relative(metadata_path, dataset_root),
            "error": transcript_error or segment_error,
            "finished_at": now_iso(),
        }
    )
    return record


def main() -> None:
    args = parse_args()
    dataset_root = Path(args.dataset_root).resolve()
    urls_file = Path(args.urls_file).resolve()
    urls = load_urls(urls_file)
    cookie_file = resolve_cookie_file(args.cookies)
    cookies_from_browser = parse_cookies_from_browser(args.cookies_from_browser)
    runtime = resolve_runtime(
        system_arg=args.system,
        video_workers_arg=args.video_workers,
        segment_workers_arg=args.segment_workers,
    )

    dataset_root.mkdir(parents=True, exist_ok=True)
    (dataset_root / "videos").mkdir(parents=True, exist_ok=True)
    (dataset_root / "manifests").mkdir(parents=True, exist_ok=True)
    (dataset_root / "links").mkdir(parents=True, exist_ok=True)

    if args.system != "auto" and runtime["system"] != runtime["detected_system"]:
        print(
            f"Runtime system override active: selected={runtime['system']} detected={runtime['detected_system']}"
        )

    print(
        "Runtime config:",
        json.dumps(
            {
                "system": runtime["system"],
                "detected_system": runtime["detected_system"],
                "cpu_count": runtime["cpu_count"],
                "video_workers": runtime["video_workers"],
                "segment_workers": runtime["segment_workers"],
                "ffmpeg_bin": args.ffmpeg_bin,
                "cookie_file_provided": bool(cookie_file),
                "cookies_from_browser_provided": bool(cookies_from_browser),
            }
        ),
    )

    # Preserve the exact input list used for this run.
    input_copy_path = dataset_root / "links" / f"input_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    input_copy_path.write_text("\n".join(urls) + "\n", encoding="utf-8")

    records = process_urls_batch(
        urls,
        dataset_root,
        auto_language=args.auto_language,
        cookie_file=cookie_file,
        cookies_from_browser=cookies_from_browser,
        audio_format=args.audio_format,
        audio_quality=args.audio_quality,
        include_all_transcripts=not args.skip_all_transcripts,
        overwrite=args.overwrite,
        generate_segments=not args.no_segments,
        segment_format=args.segment_format,
        segment_bitrate=args.segment_bitrate,
        segment_min_duration=args.segment_min_duration,
        segment_min_chars=args.segment_min_chars,
        segment_workers=runtime["segment_workers"],
        ffmpeg_bin=args.ffmpeg_bin,
        video_workers=runtime["video_workers"],
        label="video",
    )

    records_path = dataset_root / "manifests" / "records.jsonl"
    failures_path = dataset_root / "manifests" / "failures.jsonl"
    csv_path = dataset_root / "manifests" / "records.csv"
    summary_path = dataset_root / "manifests" / "summary.json"

    failed_records = [row for row in records if row.get("status") in {"failed", "partial"}]
    success_count = len([row for row in records if row.get("status") == "success"])
    partial_count = len([row for row in records if row.get("status") == "partial"])
    failed_count = len([row for row in records if row.get("status") == "failed"])

    write_jsonl(records_path, records)
    write_jsonl(failures_path, failed_records)
    write_csv(csv_path, records)
    summary = {
        "created_at": now_iso(),
        "dataset_root": str(dataset_root),
        "system": runtime["system"],
        "detected_system": runtime["detected_system"],
        "cpu_count": runtime["cpu_count"],
        "video_workers": runtime["video_workers"],
        "segment_workers": runtime["segment_workers"],
        "ffmpeg_bin": args.ffmpeg_bin,
        "cookie_file_provided": bool(cookie_file),
        "cookies_from_browser_provided": bool(cookies_from_browser),
        "total_urls": len(records),
        "success_count": success_count,
        "partial_count": partial_count,
        "failed_count": failed_count,
        "records_path": str(records_path),
        "failures_path": str(failures_path),
        "csv_path": str(csv_path),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\nRun complete")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
