const API_BASE = "/api";

async function fetchJSON<T>(endpoint: string): Promise<T> {
  const res = await fetch(`${API_BASE}${endpoint}`);
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

async function sendJSON<T>(endpoint: string, method: string, body?: unknown): Promise<T> {
  const res = await fetch(`${API_BASE}${endpoint}`, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

// ── Types ─────────────────────────────────────────────────────

export interface OverviewMetrics {
  total_posts: number;
  total_enriched: number;
  total_profiles: number;
  enrichment_pct: number;
  avg_admiralty_score: number;
  high_signal_count: number;
}

export interface SignalRow {
  post_id: string;
  owner_username: string;
  creator_id: number | null;
  platform: string;
  admiralty: string;
  gold_domain: string;
  gold_topic: string;
  is_educational: boolean;
  is_actionable: boolean;
  caption: string;
  likes_count: number;
  comments_count: number;
  video_view_count: number;
}
export interface PostRow {
  post_id: string;
  owner_username: string;
  creator_id: number | null;
  platform: string;
  caption: string;
  likes_count: number;
  comments_count: number;
  video_view_count: number;
  relative_performance: "hot" | "standout" | null;
  /** Creator's typical likes when this post was published (trailing point-in-time baseline). */
  baseline_likes: number | null;
  is_educational: boolean | null;
  is_actionable: boolean | null;
  admiralty: string | null;
  gold_domain: string | null;
  gold_topic: string | null;
  gold_subtopic: string | null;
  content_type: string | null;
  style: string | null;
  format: string | null;
  analysed_at: string | null;
  timestamp: string;
  shortcode: string;
}

export interface StandoutRow {
  post_id: string;
  owner_username: string;
  creator_id: number | null;
  platform: string;
  shortcode: string;
  caption: string;
  likes_count: number;
  comments_count: number;
  video_view_count: number;
  timestamp: string | null;
  /** Per-post TRAILING Tukey Q3 from the label pass — not a mean. */
  baseline_q3: number;
  /** Trailing Tukey IQR (baseline spread) for the same post. */
  baseline_iqr: number;
  z_score: number;
  /** How many times the post's likes exceed its point-in-time baseline. */
  breakout_multiple: number | null;
}


export interface WeeklySummaryRow {
  day: number;
  standout_count: number;
}

export interface Creator {
  id: number;
  name: string;
  created_at: string;
  updated_at: string;
  profile_count: number;
  platforms: string[];
  total_posts: number;
  standout_count: number;
  hot_count: number;
  avatar_handle: string | null;
  enriched_posts: number;
  educational_rate: number;
  actionable_rate: number;
  admiralty_score: number;
  avg_likes: number;
  max_likes: number;
  /** Mean member-post engagement score (baseline-normalized); null when no scored posts. */
  avg_engagement_score: number | null;
  /** Most frequent gold domain across the creator's posts; null when no topic data. */
  dominant_domain: string | null;
  dominant_domain_posts: number;
  /** Recent-window avg likes divided by prior-window baseline avg (point-in-time, not all-time). */
  momentum_ratio: number | null;
  is_rising: boolean;
}

export interface CreatorProfile {
  platform: string;
  handle: string;
  profile_url: string;
  results_type: string;
  results_limit: number;
  enabled: number;
  tier: string;
  creator_id: number;
  updated_at: string;
  post_count?: number;
  full_name?: string | null;
  biography?: string | null;
  followers_count?: number;
  posts_count?: number;
}

export interface CreatorMetrics {
  total_posts: number;
  avg_likes: number | null;
  avg_engagement_score: number | null;
  dominant_domain: string | null;
  dominant_domain_posts: number;
  momentum_ratio: number | null;
  is_rising: boolean;
  standout_count: number;
  hot_count: number;
}

export interface CreatorDetail {
  id: number;
  name: string;
  created_at: string;
  updated_at: string;
  profiles: CreatorProfile[];
  metrics: CreatorMetrics | null;
}

export interface ProfileInput {
  platform?: string;
  handle: string;
  results_type?: string;
  results_limit?: number;
  enabled?: boolean;
  tier?: string;
}

export interface BatchProfilesInput {
  platform?: string;
  handles: string[];
  results_type?: string;
  results_limit?: number;
  enabled?: boolean;
  tier?: string;
}


// ── Analytics API ──────────────────────────────────────────────
export async function fetchOverview(): Promise<OverviewMetrics> {
  return fetchJSON("/overview");
}

export async function fetchSignals(): Promise<SignalRow[]> {
  return fetchJSON("/signals");
}

/** One bounded page from the server-side /api/posts endpoint. */
export interface PostsPage {
  rows: PostRow[];
  total: number;
  limit: number;
  offset: number;
}

/** Fetch a single bounded page of posts (server returns {rows,total,limit,offset}). */
export async function fetchPostsPage(
  limit: number,
  offset: number,
  username?: string,
): Promise<PostsPage> {
  const params = new URLSearchParams({ limit: String(limit), offset: String(offset) });
  if (username) params.set(username, username);
  return fetchJSON(`/posts?${params.toString()}`);
}

const PAGE_FETCH_LIMIT = 5000; // server-side cap per request
const MAX_PAGES = 8; // safety bound: 5000*8 = 40k posts max

/** Fetch the full post list as a working set by paging through bounded
 *  server responses (server caps each request at PAGE_FETCH_LIMIT). The grid
 *  keeps its client row model over this set. */
export async function fetchPostsAll(username?: string): Promise<PostRow[]> {
  const rows: PostRow[] = [];
  let offset = 0;
  for (let i = 0; i < MAX_PAGES; i++) {
    const page = await fetchPostsPage(PAGE_FETCH_LIMIT, offset, username);
    rows.push(...page.rows);
    offset += page.rows.length;
    if (offset >= page.total || page.rows.length === 0) break;
  }
  return rows;
}

export async function fetchPosts(limit = 100, offset = 0): Promise<PostRow[]> {
  // Backward-compatible: fetch the requested page's rows (or all pages when
  // the caller passed the historical all-rows sentinel limit<=0).
  if (limit <= 0) return fetchPostsAll();
  return (await fetchPostsPage(limit, offset)).rows;
}

export async function fetchPostsByProfile(username: string): Promise<PostRow[]> {
  return fetchPostsAll(username);
}

export async function fetchStandoutPosts(limit = 20): Promise<StandoutRow[]> {
  return fetchJSON(`/standout-posts?limit=${limit}`);
}

export async function fetchWeeklySummary(): Promise<WeeklySummaryRow[]> {
  return fetchJSON("/weekly-summary");
}

export async function fetchHotPosts(limit = 10): Promise<StandoutRow[]> {
  return fetchJSON(`/hot-posts?limit=${limit}`);
}


// ── Post detail ────────────────────────────────────────────────

export interface PostEnrichment {
  admiralty: string | null;
  gold_domain: string | null;
  gold_subdomain: string | null;
  gold_topic: string | null;
  gold_subtopic: string | null;
  content_type: string | null;
  style: string | null;
  format: string | null;
  is_educational: boolean | null;
  is_actionable: boolean | null;
  analysed_at: string | null;
}

export interface PostPointInTime {
  label: string | null;
  is_provisional: boolean | null;
  likes_zscore: number | null;
  baseline_q3: number | null;
  baseline_iqr: number | null;
  breakout_multiple: number | null;
  sigma_tier: string | null;
  is_standout: boolean | null;
  is_hot: boolean | null;
  relative_performance: string | null;
  owner_rank: number | null;
  is_top3_in_owner: boolean | null;
}

export interface PostDetail {
  post_id: string;
  shortcode: string;
  url: string | null;
  owner_username: string | null;
  creator_id: number | null;
  creator_name: string | null;
  caption: string;
  /** Transcript not yet available in the warehouse — always null for now. */
  transcript: string | null;
  timestamp: string | null;
  likes_count: number;
  comments_count: number;
  video_view_count: number;
  media_count: number;
  hashtags: string;
  enrichment: PostEnrichment;
  platform: string;
  point_in_time: PostPointInTime;
}

/** Fetch a single post's full context. Returns null on 404 (unknown post). */
export async function fetchPostDetail(postId: string): Promise<PostDetail | null> {
  const res = await fetch(`${API_BASE}/posts/${encodeURIComponent(postId)}`);
  if (res.status === 404) return null;
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  return res.json();
}
export async function fetchSearchResults(q: string): Promise<PostRow[]> {
  return fetchJSON(`/search?q=${encodeURIComponent(q)}&limit=500`);
}

// ── Creators + profiles (profile management) ──────────────────

export async function fetchCreators(): Promise<Creator[]> {
  return fetchJSON("/creators");
}

// ── Top / Rising Creators ─────────────────────────────────────

export interface TopCreatorRow {
  creator_id: number;
  creator_name: string;
  total_posts: number;
  enriched_posts: number;
  admiralty_score: number;
  educational_rate: number;
  actionable_rate: number;
  avg_likes: number;
  max_likes: number;
  composite_score: number;
}

export interface RisingCreatorRow {
  creator_id: number;
  creator_name: string;
  recent_avg: number;
  recent_posts: number;
  baseline_avg: number;
  baseline_posts: number;
  momentum_ratio: number;
  dominant_domain: string | null;
  dominant_domain_posts: number;
  /** Topic rows with count_rank <= 5 (from v_creator_topics). */
  topics_by_count: CreatorTopicRow[];
  /** Topic rows with perf_rank <= 5 (baseline-normalized weighted engagement). */
  topics_by_perf: CreatorTopicRow[];
}

export interface CreatorTopicRow {
  topic: string;
  post_count: number;
  /** Mean member-post engagement score; null when no scored posts. */
  perf_score: number | null;
  perf_rank: number;
  count_rank: number;
}

export async function fetchTopCreators(): Promise<TopCreatorRow[]> {
  return fetchJSON("/top-creators");
}

export async function fetchRisingCreators(): Promise<RisingCreatorRow[]> {
  return fetchJSON("/rising-creators");
}

export async function fetchCreator(id: number | string): Promise<CreatorDetail> {
  return fetchJSON(`/creators/${id}`);
}

export async function fetchCreatorPosts(id: number | string): Promise<PostRow[]> {
  return fetchJSON(`/creators/${id}/posts`);
}
export async function fetchCreatorTopics(
  id: number | string,
): Promise<CreatorTopicRow[]> {
  return fetchJSON(`/creators/${id}/topics`);
}

export async function addCreator(name: string): Promise<{ id: number; name: string }> {
  return sendJSON("/creators", "POST", { name });
}

export async function renameCreator(id: number, name: string): Promise<unknown> {
  return sendJSON(`/creators/${id}`, "PATCH", { name });
}

export async function removeCreator(id: number): Promise<unknown> {
  return sendJSON(`/creators/${id}`, "DELETE");
}

export async function addProfile(
  creatorId: number,
  input: ProfileInput,
): Promise<unknown> {
  return sendJSON(`/creators/${creatorId}/profiles`, "POST", input);
}

export async function addProfilesBatch(
  creatorId: number,
  input: BatchProfilesInput,
): Promise<unknown> {
  return sendJSON(`/creators/${creatorId}/profiles/batch`, "POST", input);
}

export async function editDepth(
  platform: string,
  handle: string,
  resultsLimit: number,
): Promise<unknown> {
  return sendJSON(`/profiles/${platform}/${encodeURIComponent(handle)}`, "PATCH", {
    results_limit: resultsLimit,
  });
}

export async function removeProfile(
  platform: string,
  handle: string,
): Promise<unknown> {
  return sendJSON(`/profiles/${platform}/${encodeURIComponent(handle)}`, "DELETE");
}
