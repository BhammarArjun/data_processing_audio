# ASR/TTS Dataset Pipeline

This project supports:

- URL-first processing (`process.py`): URLs -> audio + transcripts + transcript-timed audio cuts
- Channel-first processing (`all_youtube.py`): channel refs -> video URLs -> same pipeline

## URL-first input

`process.py` accepts:

- Text file: one URL per line (comments allowed with `#`)
- JSON file:
  - `["url1", "url2"]`
  - or `{"urls": ["url1", "url2"]}`

Example: `urls.example.txt`

Install dependencies (important for YouTube challenge solving):

```bash
./venv/bin/python -m pip install -U -r requirements.txt
```

Run:

```bash
./venv/bin/python process.py --urls-file urls.example.txt --dataset-root dataset
```

High-throughput example (auto uses all CPU cores):

```bash
./venv/bin/python process.py \
  --urls-file urls.example.txt \
  --dataset-root dataset \
  --system linux \
  --video-workers 0 \
  --segment-workers 0
```

## Channel-first input

`all_youtube.py` accepts a text file with one channel reference per line:

- `@handle`
- `UC...` channel id
- full channel URL
- channel username

Example: `channels.example.txt`

Run:

```bash
./venv/bin/python all_youtube.py --channels-file channels.example.txt --dataset-root dataset
```

High-throughput channel run:

```bash
./venv/bin/python all_youtube.py \
  --channels-file channels.example.txt \
  --dataset-root dataset \
  --system mac \
  --channel-workers 0 \
  --video-workers 0 \
  --segment-workers 0
```

## Useful flags

- `--auto-language <lang>` force caption language (otherwise auto-generated caption is auto-detected)
- `--skip-all-transcripts` save only `default.json` + one auto transcript file
- `--system auto|mac|linux` choose runtime profile explicitly (or auto-detect)
- `--channel-workers 0` channel expansion parallelism (`all_youtube.py`)
- `--video-workers 0` video-level parallelism (`0` = all CPU cores)
- `--no-segments` skip transcript-timed audio cuts
- `--segment-workers 0` per-video segment-cut parallelism (`0` = auto)
- `--ffmpeg-bin <path_or_name>` custom ffmpeg binary (useful across Linux/mac environments)
- `--cookies /path/to/cookies.txt` pass YouTube cookies file (Netscape format)
- `--cookies-from-browser <spec>` load cookies directly from browser profile
- `--segment-format mp3|wav|flac|...` segment audio format (default: `mp3`)
- `--segment-bitrate 128k` compressed segment bitrate
- `--segment-min-duration 0.25` minimum transcript duration to keep a segment
- `--segment-min-chars 1` minimum transcript text length to keep a segment
- `--overwrite` re-download/re-generate existing files

## Output layout

```text
dataset/
  channels/
    <channel_slug>/
      videos.txt
      metadata.json
  links/
    input_YYYYMMDD_HHMMSS.txt
    channel_input_YYYYMMDD_HHMMSS.txt
    channel_video_urls_YYYYMMDD_HHMMSS.txt
  manifests/
    records.jsonl
    failures.jsonl
    records.csv
    summary.json
    channel_expansions.jsonl
    channel_records.jsonl
    channel_failures.jsonl
    channel_records.csv
    channel_summary.json
  videos/
    <video_id>/
      audio/
        source.mp3
      transcripts/
        default.json
        auto_detected_<lang>.json
        manual/<lang>.json
        auto/<lang>.json
      segments/
        index.jsonl
        000000/
          audio.mp3
          transcripts.json
      metadata.json
```

Notes:

- `video-workers * segment-workers` controls total ffmpeg cut concurrency. With defaults (`0`), the pipeline auto-tunes near available CPU cores.
- Both macOS and Linux are supported; choose `--system` explicitly when you want deterministic tuning across machines.

## Linux bot-check fix

If you see:

```text
Sign in to confirm you’re not a bot
```

### Important setup note (your VM + local Brave case)

If your Linux VM does not have your signed-in browser profile, do **not** use
`--cookies-from-browser` on the VM. Export cookies on your laptop (where Brave is
logged in), copy that file to the VM, then run with `--cookies`.

### 1) Export fresh cookies on your laptop

From this repo on your laptop:

```bash
chmod +x scripts/export_youtube_cookies.sh scripts/validate_youtube_cookies.sh
./scripts/export_youtube_cookies.sh "brave" "./cookies.youtube.txt"
./scripts/validate_youtube_cookies.sh "./cookies.youtube.txt"
```

If your Brave profile is not default, pass it explicitly (example):

```bash
./scripts/export_youtube_cookies.sh "brave:Default" "./cookies.youtube.txt"
```

### 2) Copy cookie file to Linux VM

```bash
scp ./cookies.youtube.txt <vm-user>@<vm-host>:/absolute/path/cookies.youtube.txt
```

### 3) Validate on VM before full run

```bash
./scripts/validate_youtube_cookies.sh "/absolute/path/cookies.youtube.txt" \
  "https://www.youtube.com/watch?v=PoT1MjnnTo4"
```

### 4) Run pipeline on VM with cookie file

run with cookies:

```bash
./venv/bin/python all_youtube.py \
  --channels-file channels.example.txt \
  --dataset-root dataset \
  --system linux \
  --cookies /absolute/path/cookies.youtube.txt
```

or browser cookies:

```bash
./venv/bin/python all_youtube.py \
  --channels-file channels.example.txt \
  --dataset-root dataset \
  --system linux \
  --cookies-from-browser "firefox:default-release"
```

For VM reliability, also lower download pressure:

```bash
./venv/bin/python all_youtube.py \
  --channels-file channels.example.txt \
  --dataset-root dataset \
  --system linux \
  --cookies /absolute/path/cookies.youtube.txt \
  --video-workers 2
```

The same flags also work with `process.py`.

If you still see `Requested format is not available` on many videos:

- test your cookie first with plain yt-dlp on one failing URL
- reduce parallel downloader pressure:

If `-F` only shows storyboard/image formats and warns about `n challenge solving failed`:

```bash
./venv/bin/python -m pip install -U "yt-dlp[default]==2026.2.4"
node --version
```

Then retry format listing:

```bash
./venv/bin/python -m yt_dlp --cookies "$PWD/cookies.youtube.txt" -F "https://www.youtube.com/watch?v=PoT1MjnnTo4"
```

```bash
./venv/bin/python all_youtube.py \
  --channels-file channels.example.txt \
  --dataset-root dataset \
  --system linux \
  --cookies "$PWD/cookies.txt" \
  --video-workers 4
```

Single-video smoke test (recommended before large runs):

```bash
python -m yt_dlp --version
ffmpeg -version | head -n 1
python -m yt_dlp \
  --cookies "$PWD/cookies.txt" \
  -f "bestaudio*/best" \
  --skip-download "https://www.youtube.com/watch?v=WnaTpgwMrnI"
```

If the command above fails, the issue is outside the pipeline (cookie/account/IP/session), not dataset directory logic.

## ffmpeg / ffprobe missing on VM

If you see:

```text
Postprocessing: ffprobe and ffmpeg not found
```

Install ffmpeg on the VM:

```bash
# Ubuntu/Debian
sudo apt-get update && sudo apt-get install -y ffmpeg

# RHEL/CentOS/Fedora (dnf)
sudo dnf install -y ffmpeg
```

Verify:

```bash
ffmpeg -version | head -n 1
ffprobe -version | head -n 1
```

If ffmpeg is installed in a non-standard path, pass it explicitly:

```bash
./venv/bin/python all_youtube.py \
  --channels-file channels.example.txt \
  --dataset-root dataset \
  --system linux \
  --cookies /absolute/path/cookies.youtube.txt \
  --ffmpeg-bin /absolute/path/to/ffmpeg \
  --video-workers 2
```
