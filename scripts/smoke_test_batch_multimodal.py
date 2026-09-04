"""External Integration Gate smoke for #19 batch-multimodal.

Proves the Gemini BATCH API accepts File-API-referenced media in InlinedRequest
via the new code path (gemini_batch.submit with media_files -> file Parts).
1 real image + 1 short video, submit -> poll -> retrieve.

Run directly (uses the real GEMINI_API_KEY from .env; ~$0.01, paid tier):
    uv run python scripts/smoke_test_batch_multimodal.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from dotenv import load_dotenv

load_dotenv()

IMAGE_URL = "https://www.gstatic.com/webp/gallery/1.jpg"
VIDEO_URL = "https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.mp4"

from datalake.defs.common.resources import GeminiResource  # noqa: E402
from datalake.defs.enrichment import gemini_batch  # noqa: E402
from datalake.defs.enrichment.prompts import _DEFAULT_GEMINI_MODEL  # noqa: E402


def _upload(url: str, mime: str):
    import tempfile
    import urllib.request

    from google.genai import Client

    client = Client(api_key=os.environ["GEMINI_API_KEY"])
    tmp = tempfile.mktemp(suffix=".media")
    urllib.request.urlretrieve(url, tmp)
    try:
        f = client.files.upload(file=tmp, config={"mime_type": mime})
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            f = client.files.get(name=f.name)
            if f.state.name == "ACTIVE":
                break
            time.sleep(2)
        return f.uri, f.mime_type, getattr(f, "video_metadata", None)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def main() -> None:
    if not os.environ.get("GEMINI_API_KEY"):
        print("GEMINI_API_KEY not set")
        sys.exit(1)

    gemini = GeminiResource(api_key=os.environ["GEMINI_API_KEY"])

    print("Uploading 1 image + 1 video to File API...")
    img_uri, img_mime, _ = _upload(IMAGE_URL, "image/jpeg")
    vid_uri, vid_mime, vid_meta = _upload(VIDEO_URL, "video/mp4")
    dur = getattr(vid_meta, "duration_seconds", None) if vid_meta else None
    print(f"  image -> {img_uri[:60]} ({img_mime})")
    print(f"  video -> {vid_uri[:60]} ({vid_mime}, dur={dur})")

    requests = [
        {
            "custom_key": "img1",
            "prompt": 'Return JSON {"seen":"image"} then one word for the image.',
            "media_files": [{"uri": img_uri, "mime_type": img_mime}],
        },
        {
            "custom_key": "vid1",
            "prompt": 'Return JSON {"seen":"video"} then one word for the video.',
            "media_files": [{"uri": vid_uri, "mime_type": vid_mime,
                             "duration_seconds": dur}],
        },
    ]

    print(f"Submitting batch ({_DEFAULT_GEMINI_MODEL})...")
    names = gemini_batch.submit(gemini, _DEFAULT_GEMINI_MODEL, requests,
                                display_name=f"smoke-bm-{int(time.time())}")
    print("  job(s):", names)

    # Poll until all terminal.
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        all_terminal = True
        for name in names:
            job = gemini_batch.poll(gemini, name)
            state = gemini_batch.job_state(job)
            print(f"  {name[-20:]} state={state}")
            if not gemini_batch.is_terminal(state):
                all_terminal = False
        if all_terminal:
            break
        time.sleep(15)

    results: dict = {}
    for name in names:
        results.update(gemini_batch.retrieve(gemini, name))
    print("\nRetrieved results:")
    for key, val in results.items():
        print(f"  {key}: ok={val.get('ok')} text={ (val.get('text') or '')[:120]!r}")
    if all(results.get(k, {}).get("ok") for k in ("img1", "vid1")):
        print("\nSMOKE PASS — Batch API accepted File-API media for image AND video.")
    else:
        print("\nSMOKE FAIL — see retrieved results above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
