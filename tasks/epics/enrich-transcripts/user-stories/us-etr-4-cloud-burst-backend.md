# US-ETR-4 — Pluggable transcript backend: local default + GCP-spot burst

- **Epic:** E-ENRICH-TRANSCRIPTS
- **Persona:** P1
- **Status:** Open
- **Relates to:** infra (GCP spot GPU; GCS staging bucket). ROADMAP Part 1.

## Story
**As a** pipeline operator (acting through an agent), **I want** transcription
to run through a **pluggable backend** — local faster-whisper by default, and an
**on-demand burst to a same-region GCP spot GPU** running the **same
faster-whisper model** when I need dev/test results in minutes or want my
machine free — **so that** iterating on transcript-served features never waits
double-digit hours, while production stays $0-local.

## Why
The full 7,029-video backfill is a ~13–35 h CPU job — fine as an unattended
production run, but too slow for dev/test iteration. A cheap GCP spot GPU
(T4/L4 class, same region as the GCS media/staging bucket) runs the *identical*
faster-whisper model in minutes for slices and ~2–6 h for the whole corpus, at
~$0.10–0.30 per dev/test run. Keeping the SAME model preserves dev == prod
parity (a different ASR like Google STT would not).

## Acceptance criteria (binary)
- AC1: A transcript **backend seam** exists with two implementations — `local`
  (faster-whisper, default) and `gcp-spot` (same model on a GCP spot GPU).
  Selecting a backend changes only the executor, never the model/output.
- AC2: The burst runs **in the same GCP region as the media/staging bucket** so
  audio transfer is internal/$0 — no cross-region egress. (Stage a slice's
  extracted audio to a GCP bucket: ingress to GCP is free; the deferred
  full-media GCS migration is NOT a prerequisite — only the slice's audio.)
- AC3: Cost guardrail on burst: prints an estimated-$ estimate before launch and
  caps GPU-hours / auto-terminates the spot instance when the job drains.
- AC4: Per-second GCP billing (1-min min) — a burst is torn down, not left
  running; no stopped-instance storage charge (contrast Vast.ai).
- AC5: Cold start bounded: pre-baked image (faster-whisper + cached model);
  prefer ONE warm instance for a whole dev/test slice over per-clip spins.

## Definition of done
- [ ] Backend seam `local | gcp-spot`; output identical across both.
- [ ] Spot burst script: stage audio → run → terminate; estimate printed at
      start; cost logged at end.
- [ ] Micro-benchmark on a real GCP spot GPU pins wall-time + $/slice before the
      full backfill.
- [ ] Tear-down guaranteed (no orphaned GPU running).

## Tests
- Same slice transcribed via `local` and `gcp-spot` yields identical transcripts
  (parity — the seam's core guarantee).
- Burst with a fault mid-run still terminates the instance (no leak).
- Cost estimate is printed before any GPU minute is billed.
