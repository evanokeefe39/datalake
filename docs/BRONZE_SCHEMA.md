# Bronze Schema — Raw Apify Data Shape

The `ig_posts_raw` asset scrapes Instagram via Apify's [Instagram Scraper](https://apify.com/apify/instagram-scraper) actor. The raw output is NDJSON; the asset writes it as typed Parquet to `data/lake/bronze/`.

## Column inventory (34 columns)

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

## Nested type details

### `latestComments` — List(Struct)
Each comment struct has: `id`, `text`, `ownerUsername`, `ownerProfilePicUrl`, `timestamp`, `repliesCount` (always null), `replies` (always null), `likesCount`, and a nested `owner` struct with `username`, `profile_pic_url`, `is_verified`, `id`, `full_name` (null), etc.

Silver discards `latestComments` entirely — it's not projected into the silver schema.

### `childPosts` — List(Struct)
Each child has its own `id`, `type`, `shortCode`, `caption`, `url`, `commentsCount`, `dimensionsHeight/Width`, `displayUrl`, and a nested `taggedUsers` list. Present only for Sidecar posts.

Silver discards `childPosts` — not projected into silver.

### `taggedUsers` / `coauthorProducers`
Both are `List(Struct{full_name, id, is_verified, profile_pic_url, username})`.

Silver does not project these.

### `musicInfo` — Struct
`{artist_name, song_name, uses_original_audio, should_mute_audio, should_mute_audio_reason, audio_id}`.

Silver does not project this.

## Post scrapes vs profile scrapes

The same Apify actor is used for both, but the input URL determines the output shape:

| Scrape type | Input URL example | `ownerUsername` | `ownerId` |
|---|---|---|---|
| Post scrape | `https://www.instagram.com/p/CODE/` | Present | Present |
| Profile scrape | `https://www.instagram.com/username/` | **null** | Present (but in `username` column) |

In profile-scraped rows, the author's handle appears in the `username` column (not `ownerUsername`). Silver handles this with a COALESCE fallback:

```python
owner_username = COALESCE("ownerUsername", "username")
```

As of 2026-07-03, 365 rows in silver have null `owner_username` — these are profile-scraped rows where `username` was also null (edge case). The `ig_posts_slv_owner_not_null` DQ check monitors this.

## Column names dropped by silver

Silver projects only 16 columns. The other 18 are discarded because they are either not needed for analysis or are too nested for DuckDB TEXT storage. Specifically:

- `latestComments`, `childPosts`, `taggedUsers`, `coauthorProducers` — nested structs, dropped
- `firstComment`, `images`, `videoUrl`, `audioUrl`, `alt`, `displayUrl` — media metadata, captured in `meta_data` JSON
- `dimensionsHeight`, `dimensionsWidth`, `videoDuration`, `musicInfo`, `isCommentsDisabled`, `locationName`, `locationId` — not needed for enrichment analysis
- `inputUrl` — redundant with `url` and `source_dataset`

## File layout

Bronze Parquet files are named by Apify run ID (e.g. `3zkcRGyHtAbMczeZG.parquet`). Each file has a `.meta` JSON sidecar with `run_id`, `actor`, `item_count`, and `created_at`.

The `ig_posts_slv` asset reads all `.parquet` files in `data/lake/bronze/` on each run, using a watermark to skip already-processed runs.

## Nested type handling

Polars List and Struct types cannot be inserted directly into DuckDB VARCHAR columns. The silver asset:

1. Serializes `List(String)` columns (`hashtags`) to JSON strings via `json.dumps()`
2. Packs remaining metadata fields (`display_url`, `video_url`, `image_urls`, `product_type`) into a `meta_data` JSON string
3. Discards deeply nested structs (`latestComments`, `childPosts`, `taggedUsers`, `coauthorProducers`, `musicInfo`)
