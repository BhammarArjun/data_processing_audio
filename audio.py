from __future__ import annotations

from pathlib import Path
from typing import Any

from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError


def _auth_opts(
    *,
    cookie_file: str | None,
    cookies_from_browser: tuple[str, str | None, str | None, str | None] | None,
) -> dict[str, Any]:
    opts: dict[str, Any] = {}
    if cookie_file:
        opts["cookiefile"] = cookie_file
    if cookies_from_browser:
        opts["cookiesfrombrowser"] = cookies_from_browser
    return opts


def _raise_with_auth_hint(error: Exception) -> None:
    message = str(error)
    if "Sign in to confirm you’re not a bot" in message or "Sign in to confirm you're not a bot" in message:
        raise RuntimeError(
            "YouTube requested bot verification. Re-run with --cookies <cookies.txt> or "
            "--cookies-from-browser <browser-spec>, e.g. --cookies-from-browser firefox:default-release."
        ) from error
    raise error


def fetch_video_info(
    url: str,
    *,
    cookie_file: str | None = None,
    cookies_from_browser: tuple[str, str | None, str | None, str | None] | None = None,
) -> dict[str, Any]:
    """Return normalized metadata for a single video URL."""
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "extract_flat": False,
        **_auth_opts(cookie_file=cookie_file, cookies_from_browser=cookies_from_browser),
    }
    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except DownloadError as exc:
        _raise_with_auth_hint(exc)
    except Exception as exc:  # noqa: BLE001
        _raise_with_auth_hint(exc)

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
    cookie_file: str | None = None,
    cookies_from_browser: tuple[str, str | None, str | None, str | None] | None = None,
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
        **_auth_opts(cookie_file=cookie_file, cookies_from_browser=cookies_from_browser),
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": audio_format,
                "preferredquality": audio_quality,
            }
        ],
    }

    try:
        with YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
    except DownloadError as exc:
        _raise_with_auth_hint(exc)
    except Exception as exc:  # noqa: BLE001
        _raise_with_auth_hint(exc)

    if target_path.exists():
        return target_path

    candidates = [path for path in output_dir.glob("source.*") if path.is_file()]
    if candidates:
        return sorted(candidates)[0]

    raise RuntimeError(f"Audio download did not produce a file for URL: {url}")
