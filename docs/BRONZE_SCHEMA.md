# Bronze Layer Contract

Bronze is a **producer-agnostic** Parquet lake in `data/lake/bronze/`. Any
producer conforming to the contract below feeds the same silver pipeline;
silver is source-agnostic. Two producers currently write here:

| Producer | Source | File naming | `source_dataset` |
|---|---|---|---|
| `ig_posts_raw` | Apify [Instagram Scraper](https://apify.com/apify/instagram-scraper) actor | `<dataset_id>.parquet` | Apify dataset id |
| `ig_posts_local_raw` | Local disk (scrape-ig-saved-list) | `local_<dataset_id>.parquet` | `local_<dataset_id>` |

## Contract (producer-agnostic)

### Storage: Parquet + `.parquet.meta` sidecar

Every bronze dataset is one Parquet file plus a JSON sidecar
`<name>.parquet.meta` carrying full lineage (run/dataset ids, actor, input
config, downloaded_at). Producers write Parquet directly with Polars —
bronze never touches DuckDB and never uses the I/O manager.

### Meta sidecar requirements

The `.meta` sidecar MUST carry `input.results_type` — one of `"posts"`,
`"details"`, `"comments"`. Silver classifies each file by this field first,
falling back to schema-sniffing only for legacy files without it. A file
with a wrong or missing `results_type` routes to the wrong (or no) silver
table.

### Watermark + write-once discovery

`ig_posts_slv` globs **all** `*.parquet` in `data/lake/bronze/` on each run
and processes files with `mtime > watermarks['silver_ig']`. Producers
therefore MUST be **write-once**: never rewrite an existing Parquet file —
an mtime bump re-triggers full silver processing of that file. New data =
new file (written atomically: temp path + rename, so the glob never sees a
half-written file).

### Dedup (source-agnostic)

Silver dedups with
`DISTINCT ON(post_id) ORDER BY scraped_at DESC, source_dataset DESC`.
Overlapping data across producers or datasets is harmless by design — the
newest scrape wins deterministically. Producers MUST emit a traceable
`source_dataset` value.

### Wire format

Both producers emit the shared 34-column Apify wire format (the local
producer maps its source JSON onto this shape, nulls where no equivalent
exists) — see [Wire-format column inventory](#wire-format-column-inventory-34-columns--shared-by-all-producers)
below.

### What a NEW producer MUST provide

1. Parquet file(s) in `data/lake/bronze/` conforming to the wire format
   (posts shape, or a distinct `results_type` shape).
2. A `.parquet.meta` sidecar carrying `input.results_type`.
3. Write-once discipline (new files only; atomic writes).
4. A traceable `source_dataset` value (e.g. `local_<dataset_id>`).
5. Nothing else. No silver changes, no new watermarks, no bootstrap or
   migration script — a new source is a producer added to this contract
   (NEW SOURCE RULE), not a one-off import.

## Wire-format column inventory (34 columns) — shared by all producers

All columns are from Apify's `stream_dataset()` output. Names use camelCase per the Apify JSON schema.

| Column | Type | Always present? | Notes |
|---|---|---|---|
| `inputUrl` | String | Yes | The URL given to the actor (profile or post URL) |
| `id` | String | Yes | Instagram post ID (the "media ID", digits only) |
| `type` | String | Yes | `"Image"`, `"Video"`, `"Sidecar"` (carousel) |
| `shortCode` | String | Yes | URL-safe shortcode (e.g. `"DY4vjexDjoI"`) |
| `caption` | String | Yes | Post caption text; may be empty |
| `hashtags` | List(String) | Yes | Extracted hashtags; may be empty list |
| `mentions` | List(String) | Yes | Extracted `@mentions`; may be empty list |
| `url` | String | Yes | Full post URL |
| `commentsCount` | Int64 | Yes | Comment count at scrape time |
| `firstComment` | String | Yes | First comment text (often engagement bait); may be `null` |
| `latestComments` | List(Struct{…}) | Yes | Up to 5 latest comments as nested structs; may be empty |
| `dimensionsHeight` | Int64 | Yes | Image/video height in pixels |
| `dimensionsWidth` | Int64 | Yes | Image/video width in pixels |
| `displayUrl` | String | Yes | URL to the display image/thumbnail |
| `images` | List(String) | Yes | All image URLs in the post; empty list for videos |
| `videoUrl` | String | No | Direct video URL; `null` for images |
| `audioUrl` | String | No | Audio track URL (Reels); `null` for non-video |
| `alt` | Null | — | Always null in observed data |
| `likesCount` | Int64 | Yes | Like count at scrape time |
| `videoViewCount` | Int64 | No | View count; present for video posts |
| `videoPlayCount` | Int64 | No | Play count; present for video posts |
| `timestamp` | String | Yes | ISO 8601 post timestamp (e.g. `"2026-06-24T11:57:03.000Z"`) |
| `childPosts` | List(Struct{…}) | No | Carousel children (Sidecar type); null for non-carousel |
| `ownerFullName` | String | Yes | Profile display name |
| `ownerUsername` | String | No | Profile `@handle`; **null for profile-scraped rows** |
| `ownerId` | String | Yes | Profile numeric ID |
| `productType` | String | Yes | `"feed"`, `"igtv"`, `"clips"`, `"carousel_container"` |
| `videoDuration` | Float64 | No | Duration in seconds; `null` for images |
| `musicInfo` | Struct | No | Music track metadata; `null` for posts without music |
| `isCommentsDisabled` | Boolean | Yes | Whether comments are turned off |
| `taggedUsers` | List(Struct) | Yes | Users tagged in the post; may be empty |
| `coauthorProducers` | List(Struct) | Yes | Co-authors; may be empty |
| `locationName` | String | No | Geotag name; `null` if no location |
| `locationId` | String | No | Geotag ID; `null` if no location |

### Nested type details

#### `latestComments` — List(Struct)
Each comment struct has: `id`, `text`, `ownerUsername`, `ownerProfilePicUrl`, `timestamp`, `repliesCount` (always null), `replies` (always null), `likesCount`, and a nested `owner` struct with `username`, `profile_pic_url`, `is_verified`, `id`, `full_name` (null), etc.

Silver discards `latestComments` entirely — it's not projected into the silver schema.

#### `childPosts` — List(Struct)
Each child has its own `id`, `type`, `shortCode`, `caption`, `url`, `commentsCount`, `dimensionsHeight/Width`, `displayUrl`, and a nested `taggedUsers` list. Present only for Sidecar posts.

Silver discards `childPosts` — not projected into silver.

#### `taggedUsers` / `coauthorProducers`
Both are `List(Struct{full_name, id, is_verified, profile_pic_url, username})`.

Silver does not project these.

#### `musicInfo` — Struct
`{artist_name, song_name, uses_original_audio, should_mute_audio, should_mute_audio_reason, audio_id}`.

Silver does not project this.

## Producer 1: ig_posts_raw (Apify)

The `ig_posts_raw` asset scrapes Instagram via Apify's [Instagram Scraper](https://apify.com/apify/instagram-scraper) actor. The raw output is NDJSON; the asset writes it as typed Parquet to `data/lake/bronze/`.

### Post scrapes vs profile scrapes

The same Apify actor is used for both, but the input URL determines the output shape:

| Scrape type | Input URL example | `ownerUsername` | `ownerId` |
|---|---|---|---|
| Post scrape | `https://www.instagram.com/p/CODE/` | Present | Present |
| Profile scrape | `https://www.instagram.com/username/` | **null** | Present (but in `username` column) |

In profile-scraped rows, the author's handle appears in the `username` column (not `ownerUsername`). Silver handles this with a COALESCE fallback in ``ig_posts_slv``:

```python
owner_username = COALESCE("ownerUsername", "username")
```

The ``ig_posts_slv_owner_not_null`` DQ check (0% tolerance) monitors for nulls.
Historical nulls are fixed by the idempotent migration ``scripts/migrate_owner_username.py``.

### File layout

Bronze Parquet files are named by Apify dataset ID (e.g. ``3zkcRGyHtAbMczeZG.parquet``).
Each file has a ``.parquet.meta`` JSON sidecar with full lineage:

```json
{
  "run_id": "abc123",
  "dataset_id": "abc123",
  "actor": "apify~instagram-scraper",
  "item_count": 5,
  "input": {
    "urls": ["https://instagram.com/..."],
    "results_limit": 12,
    "results_type": "posts"
  },
  "downloaded_at": "2026-07-03T..."
}
```

The `ig_posts_slv` asset reads all `.parquet` files in `data/lake/bronze/` on each run, using a watermark to skip already-processed runs.

### Column names dropped by silver

Silver projects only 16 columns. The other 18 are discarded because they are either not needed for analysis or are too nested for DuckDB TEXT storage. Specifically:

- `latestComments`, `childPosts`, `taggedUsers`, `coauthorProducers` — nested structs, dropped
- `firstComment`, `images`, `videoUrl`, `audioUrl`, `alt`, `displayUrl` — media metadata, captured in `meta_data` JSON
- `dimensionsHeight`, `dimensionsWidth`, `videoDuration`, `musicInfo`, `isCommentsDisabled`, `locationName`, `locationId` — not needed for enrichment analysis
- `inputUrl` — redundant with `url` and `source_dataset`

### Nested type handling

Polars List and Struct types cannot be inserted directly into DuckDB VARCHAR columns. The silver asset:

1. Serializes `List(String)` columns (`hashtags`) to JSON strings via `json.dumps()`
2. Packs remaining metadata fields (`display_url`, `video_url`, `image_urls`, `product_type`) into a `meta_data` JSON string
3. Discards deeply nested structs (`latestComments`, `childPosts`, `taggedUsers`, `coauthorProducers`, `musicInfo`)

## Producer 2: ig_posts_local_raw (local disk)

Second bronze producer (ISSUES.md #16; origin #14): ingests Instagram posts
already collected ad hoc on local disk, so the ~9,465 saved-list posts flow
through the standard medallion path without a parallel pipeline or migration
script. Full behavioral contracts in `tasks/plans/ig-local-ingestion.md`.

- **Source:** `C:/Users/evano/repos/scrape-ig-saved-list/data/ingest/<dataset_id>/<post_id>/post_metadata.json`
  — 10 local dataset ids, 9,465 posts (9,413 with media, 52 without).
- **File naming:** `local_<dataset_id>.parquet` + `local_<dataset_id>.parquet.meta`
  with `input.results_type = "posts"`. The `local_` prefix namespaces ALL
  local files uniformly, preserving provenance for the 3 dataset ids that
  collide with existing Apify ids.
- **`source_dataset`:** `local_<dataset_id>` — silver's `source_dataset DESC`
  dedup makes the overlap with Apify data harmless and deterministic.
- **Wire format:** the producer maps `post_metadata.json` onto the shared
  34-column Apify shape (nulls where the local source has no equivalent), so
  silver needs zero changes — a `local_`-prefixed Parquet with
  `results_type="posts"` is auto-picked-up by the mtime watermark.
- **Media seeding:** seed `media_cache` rows pointing at the EXISTING local
  media files — copy bytes into `POST_MEDIA_DIR` (`data/media/posts/`),
  keyed `sha256(url)`. Never re-download: the Instagram CDN URLs in these
  old posts are expired. The 52 no-media posts land with nullable media
  fields and skip seeding cleanly.
- **Ad-hoc sentinel:** `profiles.results_limit = -1` marks a profile as ad
  hoc (already ingested, not continuously scraped). `creators.py` accepts
  -1; `enabled_profiles` treats it as don't-schedule.
