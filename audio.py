from __future__ import annotations

import os
import shutil
import tempfile
from contextlib import contextmanager
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


@contextmanager
def _session_cookie_file(cookie_file: str | None):
    """
    Use an isolated cookie file per yt-dlp session.

    yt-dlp may update/dump cookies to cookiefile. With concurrent workers, sharing
    one cookie file can corrupt it. This avoids cross-worker writes.
    """
    if not cookie_file:
        yield None
        return

    source = Path(cookie_file)
    if not source.exists():
        raise FileNotFoundError(f"Cookie file not found: {source}")

    fd, temp_path = tempfile.mkstemp(prefix="yt_cookies_", suffix=".txt")
    os.close(fd)
    try:
        shutil.copyfile(source, temp_path)
        yield temp_path
    finally:
        try:
            Path(temp_path).unlink(missing_ok=True)
        except Exception:
            pass


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
    try:
        with _session_cookie_file(cookie_file) as isolated_cookie_file:
            opts = {
                "quiet": True,
                "no_warnings": True,
                "noplaylist": True,
                "extract_flat": False,
                "extractor_retries": 5,
                **_auth_opts(cookie_file=isolated_cookie_file, cookies_from_browser=cookies_from_browser),
            }
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

    try:
        with _session_cookie_file(cookie_file) as isolated_cookie_file:
            base_opts = {
                "outtmpl": str(output_dir / "source.%(ext)s"),
                "noplaylist": True,
                "quiet": True,
                "no_warnings": True,
                "overwrites": overwrite,
                "retries": 8,
                "fragment_retries": 8,
                "extractor_retries": 5,
                **_auth_opts(cookie_file=isolated_cookie_file, cookies_from_browser=cookies_from_browser),
                "postprocessors": [
                    {
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": audio_format,
                        "preferredquality": audio_quality,
                    }
                ],
            }

            format_attempts = ("bestaudio/best", "best")
            last_error: Exception | None = None
            for fmt in format_attempts:
                try:
                    with YoutubeDL({**base_opts, "format": fmt}) as ydl:
                        ydl.download([url])
                    last_error = None
                    break
                except DownloadError as exc:
                    last_error = exc
                    message = str(exc)
                    if "Requested format is not available" in message and fmt != format_attempts[-1]:
                        continue
                    _raise_with_auth_hint(exc)

            if last_error is not None:
                _raise_with_auth_hint(last_error)
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
