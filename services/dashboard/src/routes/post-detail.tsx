import { useEffect, useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Thumbnail } from "@/components/ui/thumbnail";
import { PlatformIcon } from "@/components/ui/platform-icon";
import { Avatar } from "@/components/ui/avatar";
import { admiraltyVariant, formatNumber } from "@/components/ui/posts-table";
import { fetchPostDetail, type PostDetail } from "@/lib/api";
import { ArrowLeft, ExternalLink } from "lucide-react";

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-[10px] text-muted uppercase tracking-[0.1em] font-data mb-0.5">
        {label}
      </div>
      <div className="text-sm font-data tabular-nums">{value}</div>
    </div>
  );
}

function Field({ label, value }: { label: string; value: string | null }) {
  if (!value) return null;
  return (
    <div className="flex gap-2 text-[12px]">
      <span className="text-muted w-32 flex-shrink-0">{label}</span>
      <span className="text-[#C9D4D4]">{value}</span>
    </div>
  );
}

export default function PostDetailPage() {
  const { postId } = useParams({ from: "/posts/$postId" });
  const [post, setPost] = useState<PostDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    fetchPostDetail(postId)
      .then((p) => setPost(p))
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, [postId]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-muted text-sm">
        Loading post...
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-64 gap-3">
        <p className="text-accent-red text-sm">Failed to load post: {error}</p>
        <Link
          to="/posts"
          className="flex items-center gap-1.5 text-accent text-sm hover:opacity-75"
        >
          <ArrowLeft className="w-4 h-4" /> Back to posts
        </Link>
      </div>
    );
  }

  if (!post) {
    return (
      <div className="flex flex-col items-center justify-center h-64 gap-3">
        <p className="text-muted text-sm">
          Post not found: <span className="font-data">{postId}</span>
        </p>
        <Link
          to="/posts"
          className="flex items-center gap-1.5 text-accent text-sm hover:opacity-75"
        >
          <ArrowLeft className="w-4 h-4" /> Back to posts
        </Link>
      </div>
    );
  }

  const pit = post.point_in_time;
  const enrich = post.enrichment;
  const creatorHandle = post.owner_username ?? "unknown";

  return (
    <div className="space-y-6">
      {/* ── Header: back links + source links ─────────────────── */}
      <div className="flex items-start justify-between gap-4">
        <div className="flex items-center gap-3">
          <Link
            to="/posts"
            className="flex items-center gap-1.5 text-muted text-sm hover:text-accent transition-colors"
          >
            <ArrowLeft className="w-4 h-4" /> Posts
          </Link>
          {post.creator_id != null && (
            <Link
              to="/creators/$id"
              params={{ id: String(post.creator_id) }}
              className="flex items-center gap-1.5 text-sm hover:opacity-75 transition-opacity"
              title={`View @${creatorHandle}`}
            >
              <Avatar username={creatorHandle} size={22} />
              <span className="text-cyan-400 font-semibold">@{creatorHandle}</span>
            </Link>
          )}
          {post.creator_id == null && (
            <span className="flex items-center gap-1.5 text-sm">
              <Avatar username={creatorHandle} size={22} />
              <span className="text-cyan-400 font-semibold">@{creatorHandle}</span>
            </span>
          )}
          <PlatformIcon platform={post.platform} size={16} />
        </div>
        {post.url && (
          <a
            href={post.url}
            target="_blank"
            rel="noopener noreferrer"
            title="Open post on Instagram"
            className="flex items-center gap-1.5 text-accent text-sm hover:opacity-75 transition-opacity"
          >
            Source post <ExternalLink className="w-3.5 h-3.5" />
          </a>
        )}
      </div>

      {/* ── Metadata + engagement ─────────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Post</CardTitle>
            <div className="flex items-center gap-2">
              {pit.is_hot && <Badge variant="red">Hot (2σ+)</Badge>}
              {pit.is_standout && !pit.is_hot && (
                <Badge variant="yellow">Standout</Badge>
              )}
              <Badge variant={admiraltyVariant(enrich.admiralty)}>
                {enrich.admiralty ?? "unranked"}
              </Badge>
              {pit.is_provisional && <Badge variant="default">provisional</Badge>}
            </div>
          </CardHeader>
          <div className="flex gap-4">
            {post.shortcode && (
              <a
                href={
                  post.url ??
                  `https://www.instagram.com/p/${post.shortcode}/`
                }
                target="_blank"
                rel="noopener noreferrer"
                title="Open post on Instagram"
                className="flex-shrink-0"
              >
                <Thumbnail shortcode={post.shortcode} size={160} />
              </a>
            )}
            <div className="flex-1 min-w-0 space-y-4">
              <div className="grid grid-cols-3 gap-4">
                <Metric
                  label="Likes"
                  value={post.likes_count.toLocaleString()}
                />
                <Metric
                  label="Comments"
                  value={post.comments_count.toLocaleString()}
                />
                <Metric
                  label="Views"
                  value={post.video_view_count.toLocaleString()}
                />
              </div>
              <div className="space-y-1">
                <Field label="Posted" value={post.timestamp?.slice(0, 10) ?? null} />
                <Field label="Shortcode" value={post.shortcode} />
                <Field label="Media" value={post.media_count ? `${post.media_count} item(s)` : null} />
                <Field label="Hashtags" value={post.hashtags} />
                <Field label="Creator" value={post.creator_name} />
              </div>
            </div>
          </div>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Point-in-time engagement</CardTitle>
          </CardHeader>
          {pit.likes_zscore == null ? (
            <p className="text-muted text-[12px]">
              No label-pass baseline for this post yet.
            </p>
          ) : (
            <div className="space-y-3">
              <div className="grid grid-cols-2 gap-4">
                <Metric label="Z-score" value={pit.likes_zscore.toFixed(2) + "σ"} />
                <Metric
                  label="Breakout"
                  value={
                    pit.breakout_multiple != null
                      ? `~${Math.round(pit.breakout_multiple).toLocaleString()}×`
                      : "—"
                  }
                />
                <Metric
                  label="Baseline Q3"
                  value={pit.baseline_q3 != null ? formatNumber(pit.baseline_q3) : "—"}
                />
                <Metric
                  label="Baseline IQR"
                  value={pit.baseline_iqr != null ? formatNumber(pit.baseline_iqr) : "—"}
                />
              </div>
              <p className="text-[10px] text-muted leading-snug">
                Judged against the post's own trailing baseline at publish time
                (Tukey Q3/IQR from the label pass) — not a creator all-time
                average.
                {pit.owner_rank != null && ` Rank ${pit.owner_rank} in owner's posts at that time.`}
              </p>
            </div>
          )}
        </Card>
      </div>

      {/* ── Enrichment + caption/transcript ───────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Card>
          <CardHeader>
            <CardTitle>Gold enrichment</CardTitle>
          </CardHeader>
          {enrich.analysed_at == null && enrich.gold_domain == null ? (
            <p className="text-muted text-[12px]">Not analysed yet.</p>
          ) : (
            <div className="space-y-1">
              <Field label="Domain" value={enrich.gold_domain} />
              <Field label="Subdomain" value={enrich.gold_subdomain} />
              <Field label="Topic" value={enrich.gold_topic} />
              <Field label="Subtopic" value={enrich.gold_subtopic} />
              <Field label="Content type" value={enrich.content_type} />
              <Field label="Style" value={enrich.style} />
              <Field label="Format" value={enrich.format} />
              <Field
                label="Educational"
                value={enrich.is_educational == null ? null : enrich.is_educational ? "yes" : "no"}
              />
              <Field
                label="Actionable"
                value={enrich.is_actionable == null ? null : enrich.is_actionable ? "yes" : "no"}
              />
              <Field label="Analysed" value={enrich.analysed_at?.slice(0, 10) ?? null} />
            </div>
          )}
        </Card>

        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Caption</CardTitle>
          </CardHeader>
          <p className="text-[12px] text-[#C9D4D4] whitespace-pre-wrap leading-relaxed">
            {post.caption || <span className="italic text-muted">no caption</span>}
          </p>
          <CardHeader className="mt-6">
            <CardTitle>Transcript</CardTitle>
          </CardHeader>
          {post.transcript ? (
            <p className="text-[12px] text-[#C9D4D4] whitespace-pre-wrap leading-relaxed">
              {post.transcript}
            </p>
          ) : (
            <p className="text-muted text-[12px] italic">
              Transcript not yet available for this post.
            </p>
          )}
        </Card>
      </div>
    </div>
  );
}
