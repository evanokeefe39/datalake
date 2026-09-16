import { useEffect, useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
import { Card } from "@/components/ui/card";
import { Avatar } from "@/components/ui/avatar";
import { PlatformIcon } from "@/components/ui/platform-icon";
import { PostsTable } from "@/components/ui/posts-table";
import { CreatorModal } from "@/components/ui/creator-modal";
import { Users, ArrowLeft } from "lucide-react";
import {
  fetchCreator,
  fetchCreatorPosts,
  fetchCreatorTopics,
  type CreatorDetail,
  type CreatorTopicRow,
  type PostRow,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";

export default function CreatorPage() {
  const { id } = useParams({ from: "/creators/$id" });
  const creatorId = Number(id);

  const [creator, setCreator] = useState<CreatorDetail | null>(null);
  const [posts, setPosts] = useState<PostRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [topics, setTopics] = useState<CreatorTopicRow[]>([]);
  const [postsLoading, setPostsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [modalOpen, setModalOpen] = useState(false);

  const loadCreator = () => {
    setLoading(true);
    setError(null);
    setCreator(null);
    fetchCreator(creatorId)
      .then(setCreator)
      .catch((e: unknown) => setError(String(e)))
      .finally(() => setLoading(false));
  };

  const loadPosts = () => {
    setPostsLoading(true);
    fetchCreatorPosts(creatorId)
      .then(setPosts)
      .catch((e: unknown) => setError(String(e)))
      .finally(() => setPostsLoading(false));
  };
  const loadTopics = () => {
    fetchCreatorTopics(creatorId)
      .then(setTopics)
      .catch(() => setTopics([]));
  };

  useEffect(loadCreator, [creatorId]);
  useEffect(loadTopics, [creatorId]);
  useEffect(loadPosts, [creatorId]);

  const refreshAll = () => {
    loadCreator();
    loadPosts();
  };

  if (loading) {
    return (
      <div className="text-sm text-muted font-data tracking-widest animate-pulse">
        LOADING
      </div>
    );
  }

  if (!creator) {
    return (
      <div className="space-y-4">
        <Link
          to="/creators"
          className="inline-flex items-center gap-2 text-[11px] text-muted hover:text-white transition-colors"
        >
          <ArrowLeft className="w-3.5 h-3.5" /> Back to creators
        </Link>
        <Card className="p-8 text-sm text-muted text-center">
          Creator not found.
        </Card>
      </div>
    );
  }

  const avatarHandle = creator.profiles[0]?.handle;
  const platforms = [...new Set(creator.profiles.map((p) => p.platform))];
  const m = creator.metrics;
  const byCount = topics.filter((t) => t.count_rank <= 5);
  const byPerf = topics.filter((t) => t.perf_rank <= 5);
  const momentumPct =
    m?.momentum_ratio != null
      ? Math.round((m.momentum_ratio - 1) * 100)
      : null;

  return (
    <div className="space-y-6">
      <Link
        to="/creators"
        className="inline-flex items-center gap-2 text-[11px] text-muted hover:text-white transition-colors"
      >
        <ArrowLeft className="w-3.5 h-3.5" /> Back to creators
      </Link>

      <div className="flex items-center gap-4">
        {avatarHandle ? (
          <Avatar username={avatarHandle} size={48} />
        ) : (
          <div className="w-12 h-12 border border-border flex items-center justify-center">
            <Users className="w-5 h-5 text-muted" />
          </div>
        )}
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <h2 className="text-xl font-bold tracking-[-0.03em] text-white">
              {creator.name}
            </h2>
            {platforms.map((p) => (
              <PlatformIcon key={p} platform={p} size={18} />
            ))}
          </div>
          <p className="text-[11px] text-muted font-data mt-1">
            {creator.profiles.length} profile{creator.profiles.length === 1 ? "" : "s"}
          </p>
        </div>
        <button
          onClick={() => setModalOpen(true)}
          className="border border-accent text-accent px-4 py-2 text-sm hover:bg-accent hover:text-black transition-colors"
        >
          Edit creator
        </button>
      </div>

      {m && (
        <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-6 gap-3">
          <div className="bg-surface border border-border p-3">
            <div className="text-[9px] text-muted font-data uppercase tracking-widest">
              Posts
            </div>
            <div className="text-lg font-data tabular-nums">
              {m.total_posts.toLocaleString()}
            </div>
          </div>
          <div className="bg-surface border border-border p-3">
            <div className="text-[9px] text-muted font-data uppercase tracking-widest">
              Momentum
            </div>
            <div className="flex items-center gap-1.5 mt-0.5">
              {m.is_rising ? (
                <Badge variant="green">Rising</Badge>
              ) : momentumPct != null ? (
                <span className="text-lg font-data tabular-nums">
                  {momentumPct > 0 ? "+" : ""}
                  {momentumPct}%
                </span>
              ) : (
                <span className="text-muted">—</span>
              )}
            </div>
            <div className="text-[9px] text-muted font-data mt-1">
              recent vs own prior-window baseline
            </div>
          </div>
          <div className="bg-surface border border-border p-3">
            <div className="text-[9px] text-muted font-data uppercase tracking-widest">
              Dominant Domain
            </div>
            {m.dominant_domain ? (
              <div className="mt-0.5">
                <div className="text-sm">{m.dominant_domain}</div>
                <div className="text-[9px] text-muted font-data mt-1">
                  {m.dominant_domain_posts} post
                  {m.dominant_domain_posts === 1 ? "" : "s"}
                </div>
              </div>
            ) : (
              <div className="text-muted mt-0.5">—</div>
            )}
          </div>
          <div className="bg-surface border border-border p-3">
            <div className="text-[9px] text-muted font-data uppercase tracking-widest">
              Avg Engagement
            </div>
            {m.avg_engagement_score != null ? (
              <div className="text-lg font-data tabular-nums">
                {m.avg_engagement_score.toFixed(2)}
              </div>
            ) : (
              <div className="text-muted mt-0.5">—</div>
            )}
            <div className="text-[9px] text-muted font-data mt-1">
              baseline-normalized, scored posts
            </div>
          </div>
          <div className="bg-surface border border-border p-3">
            <div className="text-[9px] text-muted font-data uppercase tracking-widest">
              Standouts
            </div>
            <div className="text-lg font-data tabular-nums text-accent-yellow">
              {m.standout_count}
            </div>
          </div>
          <div className="bg-surface border border-border p-3">
            <div className="text-[9px] text-muted font-data uppercase tracking-widest">
              Hot
            </div>
            <div className="text-lg font-data tabular-nums text-accent-red">
              {m.hot_count}
            </div>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {(
          [
            ["Top Topics by Post Count", byCount, (t: CreatorTopicRow) => `${t.post_count} post${t.post_count === 1 ? "" : "s"}`],
            ["Top Topics by Engagement Performance", byPerf, (t: CreatorTopicRow) => t.perf_score != null ? `score ${t.perf_score.toFixed(2)}` : "unscored"],
          ] as const
        ).map(([title, rows, detail]) => (
          <div key={title} className="bg-surface border border-border p-4">
            <h3 className="text-[10px] text-muted font-data uppercase tracking-widest mb-2">
              {title}
            </h3>
            {rows.length === 0 ? (
              <p className="text-muted text-xs">No topic data yet.</p>
            ) : (
              <ol className="space-y-1.5">
                {rows.map((t) => (
                  <li
                    key={t.topic}
                    className="flex items-center justify-between text-xs"
                  >
                    <span>{t.topic}</span>
                    <span className="font-data tabular-nums text-muted">
                      {detail(t)}
                    </span>
                  </li>
                ))}
              </ol>
            )}
          </div>
        ))}
      </div>

      {error && <p className="text-red-400 text-xs">{error}</p>}

      <div className="space-y-3">
        <h3 className="text-[10px] text-muted font-data uppercase tracking-widest">
          Posts
        </h3>
        {postsLoading && posts.length === 0 ? (
          <div className="flex items-center justify-center h-64 text-muted font-data text-xs tracking-widest animate-pulse">
            LOADING POSTS
          </div>
        ) : (
          <PostsTable
            rows={posts}
            pagination
            hideProfileColumn
          />
        )}
      </div>

      <CreatorModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        onSaved={refreshAll}
        creator={creator}
      />
    </div>
  );
}
