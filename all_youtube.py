from __future__ import annotations

import argparse
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import scrapetube

from process import (
    now_iso,
    parse_cookies_from_browser,
    process_urls_batch,
    resolve_cookie_file,
    resolve_runtime,
    write_csv,
    write_jsonl,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Channel-first YouTube dataset pipeline for ASR/TTS (links -> audio -> captions -> segments)."
    )
    parser.add_argument(
        "--channels-file",
        required=True,
        help="Path to a .txt file with one channel reference per line (@handle, UC... id, URL, or username).",
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
        "--channel-workers",
        type=int,
        default=0,
        help="Parallel channel expansion workers. 0 means auto.",
    )
    parser.add_argument(
        "--max-videos-per-channel",
        type=int,
        default=None,
        help="Optional cap on number of videos fetched per channel.",
    )
    parser.add_argument(
        "--sort-by",
        default="newest",
        choices=["newest", "oldest", "popular"],
        help="Video ordering when fetching channel videos. Default: newest",
    )
    parser.add_argument(
        "--auto-language",
        default=None,
        help="Optional caption language code. If omitted, generated caption language is auto-detected.",
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


def load_channels_file(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Channel file not found: {path}")

    lines = path.read_text(encoding="utf-8").splitlines()
    refs = [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]
    seen = set()
    unique_refs: list[str] = []
    for ref in refs:
        if ref not in seen:
            seen.add(ref)
            unique_refs.append(ref)
    return unique_refs


def slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip().strip("/"))
    return slug.strip("-") or "channel"


def channel_slug(channel_ref: str, index: int) -> str:
    return f"{index:04d}_{slugify(channel_ref)}"


def resolve_channel_kwargs(channel_ref: str) -> dict[str, Any]:
    if channel_ref.startswith("http://") or channel_ref.startswith("https://"):
        return {"channel_url": channel_ref}
    if channel_ref.startswith("@"):
        return {"channel_url": f"https://www.youtube.com/{channel_ref}"}
    if channel_ref.startswith("UC"):
        return {"channel_id": channel_ref}
    return {"channel_username": channel_ref}


def fetch_channel_video_urls(
    channel_ref: str,
    *,
    limit: int | None,
    sort_by: str,
) -> tuple[list[str], dict[str, Any]]:
    kwargs = resolve_channel_kwargs(channel_ref)
    videos = scrapetube.get_channel(
        limit=limit,
        sort_by=sort_by,  # type: ignore[arg-type]
        content_type="videos",
        **kwargs,
    )

    urls: list[str] = []
    for video in videos:
        video_id = video.get("videoId")
        if video_id:
            urls.append(f"https://www.youtube.com/watch?v={video_id}")

    deduped_urls: list[str] = []
    seen = set()
    for url in urls:
        if url not in seen:
            seen.add(url)
            deduped_urls.append(url)

    meta = {"source_ref": channel_ref, "resolver": kwargs, "video_count": len(deduped_urls)}
    return deduped_urls, meta


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    dataset_root = Path(args.dataset_root).resolve()
    channels_file = Path(args.channels_file).resolve()
    channel_refs = load_channels_file(channels_file)
    cookie_file = resolve_cookie_file(args.cookies)
    cookies_from_browser = parse_cookies_from_browser(args.cookies_from_browser)
    runtime = resolve_runtime(
        system_arg=args.system,
        video_workers_arg=args.video_workers,
        segment_workers_arg=args.segment_workers,
    )

    if args.channel_workers < 0:
        raise ValueError("--channel-workers must be >= 0.")
    channel_workers = args.channel_workers if args.channel_workers > 0 else runtime["video_workers"]
    channel_workers = max(1, channel_workers)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    links_dir = dataset_root / "links"
    manifests_dir = dataset_root / "manifests"
    channels_dir = dataset_root / "channels"
    videos_dir = dataset_root / "videos"
    links_dir.mkdir(parents=True, exist_ok=True)
    manifests_dir.mkdir(parents=True, exist_ok=True)
    channels_dir.mkdir(parents=True, exist_ok=True)
    videos_dir.mkdir(parents=True, exist_ok=True)

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
                "channel_workers": channel_workers,
                "video_workers": runtime["video_workers"],
                "segment_workers": runtime["segment_workers"],
                "ffmpeg_bin": args.ffmpeg_bin,
                "cookie_file_provided": bool(cookie_file),
                "cookies_from_browser_provided": bool(cookies_from_browser),
            }
        ),
    )

    (links_dir / f"channel_input_{run_id}.txt").write_text("\n".join(channel_refs) + "\n", encoding="utf-8")

    channel_rows: list[dict[str, Any] | None] = [None] * len(channel_refs)
    channel_urls_by_index: list[list[str] | None] = [None] * len(channel_refs)

    def expand_channel(index: int, channel_ref: str) -> tuple[int, dict[str, Any], list[str]]:
        slug = channel_slug(channel_ref, index + 1)
        channel_root = channels_dir / slug
        channel_root.mkdir(parents=True, exist_ok=True)
        try:
            urls, channel_meta = fetch_channel_video_urls(
                channel_ref,
                limit=args.max_videos_per_channel,
                sort_by=args.sort_by,
            )
            videos_file = channel_root / "videos.txt"
            videos_file.write_text("\n".join(urls) + ("\n" if urls else ""), encoding="utf-8")
            write_json(
                channel_root / "metadata.json",
                {
                    **channel_meta,
                    "channel_slug": slug,
                    "fetched_at": now_iso(),
                    "videos_file": str(videos_file.resolve()),
                },
            )
            row = {
                "channel_ref": channel_ref,
                "channel_slug": slug,
                "status": "success",
                "video_count": len(urls),
                "error": None,
            }
            return index, row, urls
        except Exception as exc:  # noqa: BLE001
            write_json(
                channel_root / "metadata.json",
                {
                    "source_ref": channel_ref,
                    "channel_slug": slug,
                    "status": "failed",
                    "error": str(exc),
                    "fetched_at": now_iso(),
                },
            )
            row = {
                "channel_ref": channel_ref,
                "channel_slug": slug,
                "status": "failed",
                "video_count": 0,
                "error": str(exc),
            }
            return index, row, []

    if channel_workers <= 1:
        for idx, ref in enumerate(channel_refs):
            out_idx, row, urls = expand_channel(idx, ref)
            channel_rows[out_idx] = row
            channel_urls_by_index[out_idx] = urls
            print(f"[channel {out_idx + 1}/{len(channel_refs)}] {ref} -> {row['status']} ({len(urls)} videos)")
    else:
        with ThreadPoolExecutor(max_workers=channel_workers) as executor:
            futures = {
                executor.submit(expand_channel, idx, ref): (idx, ref)
                for idx, ref in enumerate(channel_refs)
            }
            completed = 0
            for future in as_completed(futures):
                idx, ref = futures[future]
                completed += 1
                out_idx, row, urls = future.result()
                channel_rows[out_idx] = row
                channel_urls_by_index[out_idx] = urls
                print(f"[channel {completed}/{len(channel_refs)}] {ref} -> {row['status']} ({len(urls)} videos)")

    all_urls: list[str] = []
    seen_urls = set()
    for urls in channel_urls_by_index:
        for url in (urls or []):
            if url not in seen_urls:
                seen_urls.add(url)
                all_urls.append(url)

    expanded_links_path = links_dir / f"channel_video_urls_{run_id}.txt"
    expanded_links_path.write_text("\n".join(all_urls) + ("\n" if all_urls else ""), encoding="utf-8")

    records = process_urls_batch(
        all_urls,
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

    channel_records_path = manifests_dir / "channel_expansions.jsonl"
    video_records_path = manifests_dir / "channel_records.jsonl"
    video_failures_path = manifests_dir / "channel_failures.jsonl"
    video_csv_path = manifests_dir / "channel_records.csv"
    summary_path = manifests_dir / "channel_summary.json"

    failed_video_rows = [row for row in records if row.get("status") in {"failed", "partial"}]
    success_count = len([row for row in records if row.get("status") == "success"])
    partial_count = len([row for row in records if row.get("status") == "partial"])
    failed_count = len([row for row in records if row.get("status") == "failed"])

    normalized_channel_rows = [row for row in channel_rows if row is not None]
    write_jsonl(channel_records_path, normalized_channel_rows)
    write_jsonl(video_records_path, records)
    write_jsonl(video_failures_path, failed_video_rows)
    write_csv(video_csv_path, records)

    summary = {
        "created_at": now_iso(),
        "dataset_root": str(dataset_root),
        "system": runtime["system"],
        "detected_system": runtime["detected_system"],
        "cpu_count": runtime["cpu_count"],
        "channel_workers": channel_workers,
        "video_workers": runtime["video_workers"],
        "segment_workers": runtime["segment_workers"],
        "ffmpeg_bin": args.ffmpeg_bin,
        "cookie_file_provided": bool(cookie_file),
        "cookies_from_browser_provided": bool(cookies_from_browser),
        "channels_total": len(channel_refs),
        "channels_succeeded": len([row for row in normalized_channel_rows if row["status"] == "success"]),
        "channels_failed": len([row for row in normalized_channel_rows if row["status"] == "failed"]),
        "videos_total": len(all_urls),
        "videos_success": success_count,
        "videos_partial": partial_count,
        "videos_failed": failed_count,
        "expanded_links_file": str(expanded_links_path),
        "channel_records_path": str(channel_records_path),
        "video_records_path": str(video_records_path),
        "video_failures_path": str(video_failures_path),
        "video_csv_path": str(video_csv_path),
    }
    write_json(summary_path, summary)

    print("\nRun complete")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
