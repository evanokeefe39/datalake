import { Link } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import { MetricCard } from "@/components/ui/metric-card";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { DataTable } from "@/components/ui/data-table";
import { Badge } from "@/components/ui/badge";
import { Avatar } from "@/components/ui/avatar";
import { StandoutChart } from "@/components/ui/standout-chart";
import { Thumbnail } from "@/components/ui/thumbnail";
import { PlatformIcon } from "@/components/ui/platform-icon";
import {
  fetchOverview,
  fetchTopCreators,
  fetchRisingCreators,
  fetchHotPosts,
  type OverviewMetrics,
  type TopCreatorRow,
  type RisingCreatorRow,
  type StandoutRow,
} from "@/lib/api";
import { admiraltyTier } from "@/lib/admiralty";
import { TrendingUp, Trophy, Zap } from "lucide-react";

export default function OverviewPage() {
  const [metrics, setMetrics] = useState<OverviewMetrics | null>(null);
  const [top, setTop] = useState<TopCreatorRow[]>([]);
  const [rising, setRising] = useState<RisingCreatorRow[]>([]);
  const [hots, setHots] = useState<StandoutRow[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetchOverview(),
      fetchTopCreators(),
      fetchRisingCreators(),
      fetchHotPosts(12),
    ])
      .then(([m, t, r, s]) => {
        setMetrics(m);
        setTop(t);
        setRising(r);
        setHots(s);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-muted font-data text-xs tracking-widest animate-pulse">
        INITIALIZING
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* ── Metric Grid ────────────────────────────────── */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
        <MetricCard
          label="Total Posts"
          value={metrics?.total_posts?.toLocaleString() ?? "--"}
          accent="accent"
        />
        <MetricCard
          label="Enriched"
          value={metrics?.total_enriched?.toLocaleString() ?? "--"}
          sub={
            metrics?.enrichment_pct != null
              ? `${metrics.enrichment_pct}%`
              : undefined
          }
          accent="green"
        />
        <MetricCard label="Profiles" value={metrics?.total_profiles?.toLocaleString() ?? "--"} accent="magenta" />
        <MetricCard
          label="Admiralty Tier"
          value={admiraltyTier(metrics?.avg_admiralty_score)}
          sub={metrics?.avg_admiralty_score != null ? `avg ${metrics.avg_admiralty_score.toFixed(1)}` : undefined}
          accent="yellow"
        />
        <MetricCard label="High Signal" value={metrics?.high_signal_count?.toLocaleString() ?? "--"} accent="orange" />
      </div>

      {/* ── Chart + Hot Posts ─────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2">
          <StandoutChart />
        </div>

        <Card>
          <CardHeader>
            <div>
              <CardTitle>Hot Posts</CardTitle>
              <span className="text-[10px] text-muted font-data">
                {hots.length} posts &gt;2&sigma; above creator mean
              </span>
            </div>
            <Zap className="w-3.5 h-3.5 text-accent-yellow" />
          </CardHeader>
          <div className="space-y-3 max-h-[420px] overflow-y-auto">
            {hots.slice(0, 8).map((s) => (
              <div
                key={s.post_id}
                className="flex gap-3 p-2 border border-border hover:border-accent-dim transition-colors"
              >
                <a
                  href={`https://www.instagram.com/p/${s.shortcode}/`}
                  target="_blank"
                  rel="noopener noreferrer"
                  title="Open post on Instagram"
                  className="flex-shrink-0"
                >
                  <Thumbnail shortcode={s.shortcode} size={80} />
                </a>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    {s.creator_id != null ? (
                      <Link
                        to="/creators/$id"
                        params={{ id: String(s.creator_id) }}
                        className="flex items-center gap-1.5 hover:opacity-75 transition-opacity"
                        title={`View @${s.owner_username}`}
                      >
                        <Avatar username={s.owner_username} size={18} />
                        <span className="text-[11px] font-semibold text-cyan-400">
                          {s.owner_username}
                        </span>
                      </Link>
                    ) : (
                      <span className="flex items-center gap-1.5">
                        <Avatar username={s.owner_username} size={18} />
                        <span className="text-[11px] font-semibold text-cyan-400">
                          {s.owner_username}
                        </span>
                      </span>
                    )}
                    <PlatformIcon platform={s.platform} size={16} />
                    <Badge variant={s.z_score > 5 ? "green" : "yellow"}>
                      {s.z_score.toFixed(1)}&sigma;
                    </Badge>
                  </div>
                  <a
                    href={`https://www.instagram.com/p/${s.shortcode}/`}
                    target="_blank"
                    rel="noopener noreferrer"
                    title="Open post on Instagram"
                    className="block group/post"
                  >
                    <p className="text-[11px] text-muted leading-tight line-clamp-2 mb-1.5 group-hover/post:text-[#C9D4D4] transition-colors">
                      {s.caption}
                    </p>
                    <div className="flex items-center gap-3 text-[10px] font-data text-muted">
                      <span className="text-accent-green">
                        {s.likes_count?.toLocaleString()} likes
                      </span>
                      <span>vs {Math.round(s.mean_likes).toLocaleString()} avg</span>
                    </div>
                  </a>
                </div>
              </div>
            ))}
          </div>
        </Card>
      </div>

      {/* ── Top Creators ─────────────────────────────────── */}
      <Card>
        <CardHeader>
          <div>
            <CardTitle>Top Creators</CardTitle>
            <span className="text-[10px] text-muted font-data">
              {top.length} by composite quality
            </span>
          </div>
          <Trophy className="w-3.5 h-3.5 text-accent-yellow" />
        </CardHeader>
        <div className="overflow-x-auto">
          <DataTable
            columns={[
              {
                key: "rank",
                header: "#",
                sortable: false,
                className: "w-8 text-center font-data tabular-nums text-muted",
                render: (row: TopCreatorRow & { rank: number }) => row.rank,
              },
              {
                key: "creator_name",
                header: "CREATOR",
                render: (row: TopCreatorRow & { rank: number }) => (
                  <Link
                    to="/creators/$id"
                    params={{ id: String(row.creator_id) }}
                    className="flex items-center gap-2 hover:opacity-75 transition-opacity"
                    title={`View ${row.creator_name}`}
                  >
                    <Avatar username={row.creator_name} size={24} />
                    <span className="text-cyan-400 font-semibold text-[13px]">
                      {row.creator_name}
                    </span>
                  </Link>
                ),
              },
              {
                key: "admiralty_score",
                header: "ADMIRALTY",
                className: "text-center",
                render: (row: TopCreatorRow & { rank: number }) => {
                  const tier = admiraltyTier(row.admiralty_score);
                  const variant =
                    tier === "A+" || tier === "A"
                      ? "green"
                      : tier === "B"
                        ? "yellow"
                        : tier === "C"
                          ? "orange"
                          : "default";
                  return <Badge variant={variant}>{tier}</Badge>;
                },
              },
              {
                key: "composite_score",
                header: "SCORE",
                className: "text-right font-data tabular-nums",
                render: (row: TopCreatorRow & { rank: number }) =>
                  row.composite_score?.toFixed(2) ?? "--",
              },
              {
                key: "avg_likes",
                header: "AVG LIKES",
                className: "text-right font-data tabular-nums hidden sm:table-cell",
                render: (row: TopCreatorRow & { rank: number }) =>
                  row.avg_likes?.toLocaleString() ?? "--",
              },
            ]}
            data={top.map((r, i) => ({ ...r, rank: i + 1 }))}
            rowKey={(r) => String(r.creator_id)}
          />
        </div>
      </Card>

      {/* ── Rising Creators ──────────────────────────────── */}
      <Card>
        <CardHeader>
          <div>
            <CardTitle>Rising Creators</CardTitle>
            <span className="text-[10px] text-muted font-data">
              {rising.length} accelerating vs own baseline
            </span>
          </div>
          <TrendingUp className="w-3.5 h-3.5 text-accent-green" />
        </CardHeader>
        <div className="overflow-x-auto">
          <DataTable
            columns={[
              {
                key: "rank",
                header: "#",
                sortable: false,
                className: "w-8 text-center font-data tabular-nums text-muted",
                render: (row: RisingCreatorRow & { rank: number }) => row.rank,
              },
              {
                key: "creator_name",
                header: "CREATOR",
                render: (row: RisingCreatorRow & { rank: number }) => (
                  <Link
                    to="/creators/$id"
                    params={{ id: String(row.creator_id) }}
                    className="flex items-center gap-2 hover:opacity-75 transition-opacity"
                    title={`View ${row.creator_name}`}
                  >
                    <Avatar username={row.creator_name} size={24} />
                    <span className="text-cyan-400 font-semibold text-[13px]">
                      {row.creator_name}
                    </span>
                  </Link>
                ),
              },
              {
                key: "momentum_ratio",
                header: "MOMENTUM",
                className: "text-center",
                render: (row: RisingCreatorRow & { rank: number }) => (
                  <Badge variant="green">
                    +{Math.round((row.momentum_ratio - 1) * 100)}%
                  </Badge>
                ),
              },
              {
                key: "recent_avg",
                header: "RECENT",
                className: "text-right font-data tabular-nums",
                render: (row: RisingCreatorRow & { rank: number }) =>
                  row.recent_avg?.toLocaleString() ?? "--",
              },
              {
                key: "baseline_avg",
                header: "BASELINE",
                className: "text-right font-data tabular-nums",
                render: (row: RisingCreatorRow & { rank: number }) =>
                  row.baseline_avg?.toLocaleString() ?? "--",
              },
            ]}
            data={rising.map((r, i) => ({ ...r, rank: i + 1 }))}
            rowKey={(r) => String(r.creator_id)}
            emptyMessage="No creator has >=3 posts in both the recent 4w and prior 8w windows yet."
          />
        </div>
      </Card>
    </div>
  );
}
