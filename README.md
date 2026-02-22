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

run with cookies:

```bash
./venv/bin/python all_youtube.py \
  --channels-file channels.example.txt \
  --dataset-root dataset \
  --system linux \
  --cookies /absolute/path/to/cookies.txt
```

or browser cookies:

```bash
./venv/bin/python all_youtube.py \
  --channels-file channels.example.txt \
  --dataset-root dataset \
  --system linux \
  --cookies-from-browser "firefox:default-release"
```

The same flags also work with `process.py`.

If you still see `Requested format is not available` on many videos:

- test your cookie first with plain yt-dlp on one failing URL
- reduce parallel downloader pressure:

```bash
./venv/bin/python all_youtube.py \
  --channels-file channels.example.txt \
  --dataset-root dataset \
  --system linux \
  --cookies "$PWD/cookies.txt" \
  --video-workers 4
```
