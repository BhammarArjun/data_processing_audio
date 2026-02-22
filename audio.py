from __future__ import annotations

import os
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


def _default_youtube_extractor_args() -> dict[str, Any]:
    """
    Prefer client set that is usually stable for authenticated sessions.

    Avoids over-reliance on clients that often require PO tokens.
    """
    return {
        "youtube": {
            "player_client": ["tv_downgraded", "android", "ios"],
        }
    }


def _format_unavailable_error(error: Exception) -> bool:
    message = str(error).lower()
    return "requested format is not available" in message or "requested format not available" in message


def _sanitize_cookie_file_text(raw: bytes) -> str:
    # Accept imperfect transfers and normalize into Netscape-like lines.
    text = raw.decode("utf-8", errors="ignore").replace("\x00", "").replace("\r\n", "\n").replace("\r", "\n")
    allowed_domains = ("youtube.com", "google.com", "googlevideo.com", "ytimg.com")
    sanitized: list[str] = []

    for line in text.split("\n"):
        if not line:
            continue
        if line.startswith("#"):
            sanitized.append(line)
            continue

        parts = line.split("\t")
        if len(parts) < 7:
            continue

        domain = parts[0].lstrip(".").lower()
        if not any(domain.endswith(suffix) for suffix in allowed_domains):
            continue
        sanitized.append("\t".join(parts[:7]))

    if not sanitized:
        raise RuntimeError("No valid YouTube/Google cookies found in cookie file.")

    header = ["# Netscape HTTP Cookie File", "# Sanitized for yt-dlp session", ""]
    return "\n".join(header + sanitized) + "\n"


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
        raw = source.read_bytes()
        sanitized = _sanitize_cookie_file_text(raw)
        Path(temp_path).write_text(sanitized, encoding="utf-8")
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
            base_opts = {
                "quiet": True,
                "no_warnings": True,
                "noplaylist": True,
                "extract_flat": False,
                "extractor_retries": 5,
                "retries": 5,
                "js_runtimes": {"node": {}},
                "http_headers": {
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"
                    )
                },
                **_auth_opts(cookie_file=isolated_cookie_file, cookies_from_browser=cookies_from_browser),
            }
            strategy_opts = (
                {**base_opts, "extractor_args": _default_youtube_extractor_args()},
                base_opts,
            )
            info: dict[str, Any] | None = None
            last_error: Exception | None = None
            for opts in strategy_opts:
                try:
                    with YoutubeDL(opts) as ydl:
                        info = ydl.extract_info(url, download=False)
                    break
                except DownloadError as exc:
                    last_error = exc
                    message = str(exc)
                    if "Sign in to confirm you’re not a bot" in message or "Sign in to confirm you're not a bot" in message:
                        _raise_with_auth_hint(exc)
                    continue
                except Exception as exc:  # noqa: BLE001
                    # Retry once with default extractor behavior, then fail.
                    last_error = exc
                    continue
            if info is None and last_error is not None:
                _raise_with_auth_hint(last_error)
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
                "js_runtimes": {"node": {}},
                "http_headers": {
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"
                    )
                },
                **_auth_opts(cookie_file=isolated_cookie_file, cookies_from_browser=cookies_from_browser),
                "postprocessors": [
                    {
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": audio_format,
                        "preferredquality": audio_quality,
                    }
                ],
            }

            format_attempts: tuple[str | None, ...] = (
                "bestaudio[acodec!=none]/bestaudio*/bestaudio/best*[acodec!=none]/best",
                "bestaudio*/bestaudio/best",
                "best",
                None,
            )
            strategy_opts = (
                {**base_opts, "extractor_args": _default_youtube_extractor_args()},
                base_opts,
            )
            last_error: Exception | None = None
            finished = False
            for base in strategy_opts:
                for fmt in format_attempts:
                    try:
                        opts = dict(base)
                        if fmt is not None:
                            opts["format"] = fmt
                        with YoutubeDL(opts) as ydl:
                            ydl.download([url])
                        last_error = None
                        finished = True
                        break
                    except DownloadError as exc:
                        last_error = exc
                        if _format_unavailable_error(exc):
                            continue
                        _raise_with_auth_hint(exc)
                if finished:
                    break

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
