"""Server-side video frame sampling for qwen-batch-service.

Videos referenced by path in a job item's `images` list are sampled here
(N JPEG frames, evenly spaced across the video duration, largest side
<= max_side) before being inlined into the OpenRouter request. Only the
resulting frames leave the service; the client never sends media bytes.

ffmpeg/ffprobe are driven via subprocess — no python video dependency.
Sampling failures are deterministic (corrupt file, missing binary), so
they surface as TerminalQwenError and must not burn retries.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from .qwen import TerminalQwenError

VIDEO_EXTENSIONS = {".mp4", ".mov", ".webm", ".m4v"}

FRAMES_PER_VIDEO_ENV = "QWEN_BATCH_FRAMES_PER_VIDEO"
FRAME_MAX_SIDE_ENV = "QWEN_BATCH_FRAME_MAX_SIDE"
DEFAULT_FRAMES_PER_VIDEO = 8
DEFAULT_FRAME_MAX_SIDE = 1080


class VideoSamplingError(TerminalQwenError):
    """Raised when a video cannot be sampled (missing ffmpeg, unreadable file)."""


def is_video_path(path: str) -> bool:
    """True if the path's extension marks it as a video (not a native image)."""
    return Path(path).suffix.lower() in VIDEO_EXTENSIONS


def frames_per_video() -> int:
    return int(os.environ.get(FRAMES_PER_VIDEO_ENV, DEFAULT_FRAMES_PER_VIDEO))


def frame_max_side() -> int:
    return int(os.environ.get(FRAME_MAX_SIDE_ENV, DEFAULT_FRAME_MAX_SIDE))


def sample_video(
    path: str,
    *,
    n: int | None = None,
    max_side: int | None = None,
) -> list[bytes]:
    """Sample a video file into exactly n JPEG frames, evenly spaced.

    Returns raw JPEG bytes, one per frame, in chronological order. Scales
    frames so the largest side is <= max_side (aspect preserved, even
    dimensions). Raises VideoSamplingError on any failure — deterministic,
    terminal for the item.
    """
    if n is None:
        n = frames_per_video()
    if max_side is None:
        max_side = frame_max_side()
    if n < 1:
        raise VideoSamplingError(f"frames per video must be >= 1, got {n} for {path!r}")

    if shutil.which("ffmpeg") is None:
        raise VideoSamplingError(
            f"ffmpeg is not installed or not on PATH — cannot sample video {path!r}. "
            "Install ffmpeg (Docker image includes it; locally: scoop install ffmpeg)."
        )

    duration_s = _probe_duration(path)
    if duration_s <= 0:
        raise VideoSamplingError(
            f"video {path!r} reports non-positive duration ({duration_s}s); "
            "cannot sample frames"
        )
    timestamps = [(i + 0.5) * duration_s / n for i in range(n)]
    vf = (
        f"scale='trunc(min(iw,{max_side}*iw/max(iw\\,ih))/2)*2'"
        f":'trunc(min(ih,{max_side}*ih/max(iw\\,ih))/2)*2'"
    )
    frames: list[bytes] = []
    for ts in timestamps:
        cmd = [
            "ffmpeg",
            "-nostdin",
            "-v", "error",
            "-ss", f"{ts:.6f}",
            "-i", path,
            "-frames:v", "1",
            "-vf", vf,
            "-f", "image2pipe",
            "-vcodec", "mjpeg",
            "-",
        ]
        try:
            proc = subprocess.run(cmd, capture_output=True, timeout=120)
        except subprocess.TimeoutExpired as exc:
            raise VideoSamplingError(
                f"ffmpeg timed out after {exc.timeout}s sampling video {path!r}"
            ) from exc
        except OSError as exc:
            raise VideoSamplingError(f"failed to run ffmpeg for {path!r}: {exc}") from exc
        if proc.returncode != 0 or not proc.stdout:
            stderr = proc.stderr.decode("utf-8", errors="replace").strip()
            raise VideoSamplingError(
                f"ffmpeg failed to sample video {path!r} (unreadable or corrupt?): {stderr[:500]}"
            )
        frames.append(proc.stdout)
    return frames


def _probe_duration(path: str) -> float:
    if shutil.which("ffprobe") is None:
        raise VideoSamplingError(
            f"ffprobe is not installed or not on PATH — cannot sample video {path!r}. "
            "Install ffmpeg (ffprobe ships with it)."
        )
    cmd = [
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        path,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, timeout=30)
    except subprocess.TimeoutExpired as exc:
        raise VideoSamplingError(
            f"ffprobe timed out after {exc.timeout}s probing video {path!r}"
        ) from exc
    except OSError as exc:
        raise VideoSamplingError(f"failed to run ffprobe for {path!r}: {exc}") from exc
    if proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", errors="replace").strip()
        raise VideoSamplingError(
            f"ffprobe failed on video {path!r} (unreadable or corrupt?): {stderr[:500]}"
        )
    try:
        return float(proc.stdout.decode("ascii").strip())
    except ValueError as exc:
        raise VideoSamplingError(f"could not determine duration of video {path!r}") from exc
