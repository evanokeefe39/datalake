"""Unit tests for server-side video frame sampling in the qwen path.

ffmpeg/ffprobe are mocked at the subprocess boundary (no real video needed);
one real-video test runs only when ffmpeg is on PATH and a sample exists.
"""
from __future__ import annotations

import base64
import subprocess
from pathlib import Path

import pytest
from jobs import qwen, video
from jobs.qwen import TerminalQwenError
from jobs.video import VideoSamplingError


class _Proc:
    def __init__(self, stdout: bytes = b"", returncode: int = 0, stderr: bytes = b""):
        self.stdout = stdout
        self.returncode = returncode
        self.stderr = stderr


def _fake_ffmpeg_run(frames: int, *, fail: bool = False):
    """Return a subprocess.run stand-in: ffprobe -> duration, ffmpeg -> frame bytes."""
    calls = []

    def run(cmd, capture_output=True, timeout=None):
        calls.append(cmd)
        if cmd[0] == "ffprobe":
            return _Proc(stdout=b"12.5\n")
        assert cmd[0] == "ffmpeg"
        if fail:
            return _Proc(returncode=1, stderr=b"Invalid data found\n")
        return _Proc(stdout=b"\xff\xd8frame")

    run.calls = calls
    return run


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv(video.FRAMES_PER_VIDEO_ENV, raising=False)
    monkeypatch.delenv(video.FRAME_MAX_SIDE_ENV, raising=False)
    # Every test here mocks `subprocess.run`, so ffmpeg is never really invoked —
    # but `sample_video` gates on `shutil.which("ffmpeg")` BEFORE it calls the
    # mocked runner. Left real, these tests pass only on a machine that happens
    # to have ffmpeg installed and fail on a bare CI runner, which is a false
    # signal in both directions. Declare the capability instead of discovering it.
    monkeypatch.setattr(video.shutil, "which", lambda name: f"/usr/bin/{name}")
    # Same reasoning as ffmpeg above: `qwen.chat` reads the key BEFORE it posts,
    # and every test here intercepts the httpx call, so no real request is made.
    # Without this the tests demand a live credential to exercise local logic.
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key-not-used")


def test_video_path_yields_n_frame_parts(monkeypatch, tmp_path):
    vid = tmp_path / "clip.mp4"
    vid.write_bytes(b"fake")
    monkeypatch.setattr(video.subprocess, "run", _fake_ffmpeg_run(3))
    monkeypatch.setenv(video.FRAMES_PER_VIDEO_ENV, "3")

    parts = qwen.chat._build_parts.__self__ if False else None  # noqa: F841 (keep helper ref local)
    parts = _content_parts(str(vid))

    assert len(parts) == 3  # 1 text + ... see helper: text excluded, frames only
    for p in parts:
        assert p["type"] == "image_url"
        url = p["image_url"]["url"]
        assert url.startswith("data:image/jpeg;base64,")
        assert base64.b64decode(url.split(",", 1)[1]) == b"\xff\xd8frame"


def _content_parts(*images: str) -> list[dict]:
    """Reuse qwen.chat's content-building by intercepting the httpx post."""
    captured = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["payload"] = json
        return _Resp({"choices": [{"message": {"content": "ok"}}]})

    monkeypatch_httpx(fake_post)
    qwen.chat("p", list(images), model="m")
    return captured["payload"]["messages"][0]["content"][1:]


class _Resp:
    status_code = 200

    def __init__(self, body):
        self._body = body
        self.text = ""

    def json(self):
        return self._body


_monkeypatched = {}


def monkeypatch_httpx(fake_post):
    """Patch httpx.post for the duration of a test via module attribute swap."""
    _monkeypatched["orig"] = qwen.httpx.post
    qwen.httpx.post = fake_post


def teardown_function():
    if "orig" in _monkeypatched:
        qwen.httpx.post = _monkeypatched.pop("orig")


def test_native_image_passthrough_unchanged(monkeypatch, tmp_path):
    img = tmp_path / "photo.jpg"
    img.write_bytes(b"\xff\xd8native")
    parts = _content_parts(str(img))
    assert len(parts) == 1
    url = parts[0]["image_url"]["url"]
    assert url == "data:image/jpeg;base64," + base64.b64encode(b"\xff\xd8native").decode()


def test_mixed_image_and_video_order(monkeypatch, tmp_path):
    img = tmp_path / "a.png"
    img.write_bytes(b"pngbytes")
    vid = tmp_path / "b.mov"
    vid.write_bytes(b"fake")
    monkeypatch.setattr(video.subprocess, "run", _fake_ffmpeg_run(2))
    monkeypatch.setenv(video.FRAMES_PER_VIDEO_ENV, "2")
    parts = _content_parts(str(img), str(vid))
    assert parts[0]["image_url"]["url"].endswith(base64.b64encode(b"pngbytes").decode())
    assert len(parts) == 3  # image + 2 frames
    assert all(p["image_url"]["url"].startswith("data:image/") for p in parts)


def test_ffmpeg_failure_is_terminal(monkeypatch, tmp_path):
    vid = tmp_path / "broken.webm"
    vid.write_bytes(b"junk")
    monkeypatch.setattr(video.subprocess, "run", _fake_ffmpeg_run(1, fail=True))
    with pytest.raises(VideoSamplingError) as exc:
        video.sample_video(str(vid))
    assert "broken.webm" in str(exc.value)
    # Terminal semantics: subclass of the error type the worker treats as terminal.
    assert isinstance(exc.value, TerminalQwenError)


def test_ffmpeg_missing_is_terminal(monkeypatch, tmp_path):
    vid = tmp_path / "v.mp4"
    vid.write_bytes(b"fake")
    monkeypatch.setattr(video.shutil, "which", lambda name: None)
    with pytest.raises(VideoSamplingError, match="ffmpeg"):
        video.sample_video(str(vid))


def test_ffmpeg_timeout_is_terminal(monkeypatch, tmp_path):
    vid = tmp_path / "slow.mp4"
    vid.write_bytes(b"fake")

    def run(cmd, capture_output=True, timeout=None):
        if cmd[0] == "ffprobe":
            return _Proc(stdout=b"12.5\n")
        raise subprocess.TimeoutExpired(cmd, timeout)

    monkeypatch.setattr(video.subprocess, "run", run)
    with pytest.raises(VideoSamplingError) as exc:
        video.sample_video(str(vid))
    assert "slow.mp4" in str(exc.value)
    # Terminal semantics: the worker must not burn retries on a timeout.
    assert isinstance(exc.value, TerminalQwenError)


def test_nonpositive_duration_is_terminal(monkeypatch, tmp_path):
    vid = tmp_path / "zero.mp4"
    vid.write_bytes(b"fake")

    def run(cmd, capture_output=True, timeout=None):
        assert cmd[0] == "ffprobe"
        return _Proc(stdout=b"0.0\n")

    monkeypatch.setattr(video.subprocess, "run", run)
    with pytest.raises(VideoSamplingError) as exc:
        video.sample_video(str(vid))
    assert "zero.mp4" in str(exc.value)
    assert isinstance(exc.value, TerminalQwenError)


def test_env_defaults_applied(monkeypatch, tmp_path):
    vid = tmp_path / "v.m4v"
    vid.write_bytes(b"fake")
    runner = _fake_ffmpeg_run(0)
    monkeypatch.setattr(video.subprocess, "run", runner)
    frames = video.sample_video(str(vid))
    assert len(frames) == video.DEFAULT_FRAMES_PER_VIDEO
    ffmpeg_cmds = [c for c in runner.calls if c[0] == "ffmpeg"]
    assert len(ffmpeg_cmds) == video.DEFAULT_FRAMES_PER_VIDEO
    assert ",1080*" in ffmpeg_cmds[0][ffmpeg_cmds[0].index("-vf") + 1]
    # explicit overrides
    monkeypatch.setenv(video.FRAMES_PER_VIDEO_ENV, "2")
    monkeypatch.setenv(video.FRAME_MAX_SIDE_ENV, "640")
    runner2 = _fake_ffmpeg_run(0)
    monkeypatch.setattr(video.subprocess, "run", runner2)
    assert len(video.sample_video(str(vid))) == 2
    vf = runner2.calls[-1][runner2.calls[-1].index("-vf") + 1]
    assert "640*" in vf
    # frames evenly spaced across the probed duration (12.5s / n)
    if len(ffmpeg_cmds) > 2:
        ss = [float(c[c.index("-ss") + 1]) for c in ffmpeg_cmds]
        diffs = [b - a for a, b in zip(ss, ss[1:])]
        assert all(abs(d - diffs[0]) < 1e-6 for d in diffs)


@pytest.mark.skipif(
    not video.shutil.which("ffmpeg"), reason="ffmpeg not on PATH"
)
def test_real_video_sampling():
    """End-to-end check with a real generated clip (only when ffmpeg exists)."""
    import shutil as _sh
    import tempfile
    tmp = Path(tempfile.mkdtemp())
    clip = tmp / "real.mp4"
    subprocess.run(
        ["ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i",
         "testsrc=duration=2:size=320x240:rate=10", "-pix_fmt", "yuv420p", str(clip)],
        check=True, capture_output=True,
    )
    frames = video.sample_video(str(clip), n=4, max_side=160)
    assert len(frames) == 4
    assert all(f[:2] == b"\xff\xd8" for f in frames)
    _sh.rmtree(tmp, ignore_errors=True)
