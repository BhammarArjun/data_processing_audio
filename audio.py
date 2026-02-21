from __future__ import annotations

from pathlib import Path
from typing import Any

from yt_dlp import YoutubeDL


def fetch_video_info(url: str) -> dict[str, Any]:
    """Return normalized metadata for a single video URL."""
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "extract_flat": False,
    }
    with YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)

    # If a playlist-like URL is passed, use the first resolved entry.
    if isinstance(info, dict) and info.get("entries"):
        entries = [entry for entry in info["entries"] if entry]
        if entries:
            info = entries[0]

    if not isinstance(info, dict) or not info.get("id"):
        raise RuntimeError(f"Could not resolve video metadata for URL: {url}")
    return info


def download_audio(
    url: str,
    output_dir: Path,
    *,
    audio_format: str = "mp3",
    audio_quality: str = "192",
    overwrite: bool = False,
) -> Path:
    """Download best audio and convert to the requested format."""
    output_dir.mkdir(parents=True, exist_ok=True)
    target_path = output_dir / f"source.{audio_format}"

    if target_path.exists() and not overwrite:
        return target_path

    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": str(output_dir / "source.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "overwrites": overwrite,
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": audio_format,
                "preferredquality": audio_quality,
            }
        ],
    }

    with YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])

    if target_path.exists():
        return target_path

    candidates = [path for path in output_dir.glob("source.*") if path.is_file()]
    if candidates:
        return sorted(candidates)[0]

    raise RuntimeError(f"Audio download did not produce a file for URL: {url}")
