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

export interface ProfileRow {
  owner_id: string;
  owner_username: string;
  creator_id: number | null;
  total_posts: number;
  enriched_posts: number;
  admiralty_score: number;
  educational_rate: number;
  avg_likes: number;
  avg_comments: number;
  avg_video_views: number;
  max_likes: number;
}

export interface SignalRow {
  post_id: string;
  owner_username: string;
  creator_id: number | null;
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
  caption: string;
  likes_count: number;
  comments_count: number;
  video_view_count: number;
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
  shortcode: string;
  caption: string;
  likes_count: number;
  comments_count: number;
  video_view_count: number;
  timestamp: string | null;
  mean_likes: number;
  std_likes: number;
  z_score: number;
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

export interface CreatorDetail {
  id: number;
  name: string;
  created_at: string;
  updated_at: string;
  profiles: CreatorProfile[];
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

export async function fetchProfiles(): Promise<ProfileRow[]> {
  return fetchJSON("/profiles");
}

export async function fetchSignals(): Promise<SignalRow[]> {
  return fetchJSON("/signals");
}

export async function fetchPosts(limit = 100, offset = 0): Promise<PostRow[]> {
  return fetchJSON(`/posts?limit=${limit}&offset=${offset}`);
}

export async function fetchPostsByProfile(username: string): Promise<PostRow[]> {
  return fetchJSON(`/posts?username=${encodeURIComponent(username)}`);
}

export async function fetchStandoutPosts(limit = 20): Promise<StandoutRow[]> {
  return fetchJSON(`/standout-posts?limit=${limit}`);
}

export async function fetchWeeklySummary(): Promise<WeeklySummaryRow[]> {
  return fetchJSON("/weekly-summary");
}

export async function fetchRecentStandouts(limit = 10): Promise<StandoutRow[]> {
  return fetchJSON(`/recent-standouts?limit=${limit}`);
}

export async function fetchSearchResults(q: string): Promise<PostRow[]> {
  return fetchJSON(`/search?q=${encodeURIComponent(q)}&limit=500`);
}

// ── Creators + profiles (profile management) ──────────────────

export async function fetchCreators(): Promise<Creator[]> {
  return fetchJSON("/creators");
}

export async function fetchCreator(id: number | string): Promise<CreatorDetail> {
  return fetchJSON(`/creators/${id}`);
}

export async function fetchCreatorPosts(id: number | string): Promise<PostRow[]> {
  return fetchJSON(`/creators/${id}/posts`);
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
