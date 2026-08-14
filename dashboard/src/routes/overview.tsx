import { Link } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import { MetricCard } from "@/components/ui/metric-card";
import { Card, CardHeader, CardTitle } from "@/components/ui/card";
import { DataTable } from "@/components/ui/data-table";
import { Badge } from "@/components/ui/badge";
import { Avatar } from "@/components/ui/avatar";
import { StandoutChart } from "@/components/ui/standout-chart";
import { Thumbnail } from "@/components/ui/thumbnail";
import {
  fetchOverview,
  fetchProfiles,
  fetchRecentStandouts,
  type OverviewMetrics,
  type ProfileRow,
  type StandoutRow,
} from "@/lib/api";
import { Zap } from "lucide-react";

function admiraltyTier(score: number | undefined | null): string {
  if (score == null) return "--";
  if (score >= 4) return "A+";
  if (score >= 3) return "A";
  if (score >= 2) return "B";
  if (score >= 1) return "C";
  return "D";
}

export default function OverviewPage() {
  const [metrics, setMetrics] = useState<OverviewMetrics | null>(null);
  const [profiles, setProfiles] = useState<ProfileRow[]>([]);
  const [standouts, setStandouts] = useState<StandoutRow[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetchOverview(),
      fetchProfiles(),
      fetchRecentStandouts(12),
    ])
      .then(([m, p, s]) => {
        setMetrics(m);
        setProfiles(p);
        setStandouts(s);
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

      {/* ── Chart + Standouts ───────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2">
          <StandoutChart />
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Top Standouts</CardTitle>
            <Zap className="w-3.5 h-3.5 text-accent-yellow" />
          </CardHeader>
          <div className="space-y-3 max-h-[420px] overflow-y-auto">
            {standouts.slice(0, 8).map((s) => (
              <a
                key={s.post_id}
                href={`https://www.instagram.com/p/${s.shortcode}/`}
                target="_blank"
                rel="noopener noreferrer"
                title="Open on Instagram"
                className="flex gap-3 p-2 border border-border hover:border-accent-dim transition-colors group"
              >
                <Thumbnail shortcode={s.shortcode} size={80} />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    <Avatar username={s.owner_username} size={18} />
                    <span className="text-[11px] font-semibold text-cyan-400">
                      {s.owner_username}
                    </span>
                    <Badge variant={s.z_score > 5 ? "green" : "yellow"}>
                      {s.z_score.toFixed(1)}&sigma;
                    </Badge>
                  </div>
                  <p className="text-[11px] text-muted leading-tight line-clamp-2 mb-1.5">
                    {s.caption}
                  </p>
                  <div className="flex items-center gap-3 text-[10px] font-data text-muted">
                    <span className="text-accent-green">
                      {s.likes_count?.toLocaleString()} likes
                    </span>
                    <span>vs {Math.round(s.mean_likes).toLocaleString()} avg</span>
                  </div>
                </div>
              </a>
            ))}
          </div>
        </Card>
      </div>

      {/* ── Profile Quality ─────────────────────────────── */}
      <Card>
        <CardHeader>
          <CardTitle>Profile Quality</CardTitle>
          <span className="text-[10px] text-muted font-data">
            {profiles.length} with enrichment
          </span>
        </CardHeader>
        <div className="overflow-x-auto">
          <DataTable
            columns={[
              {
                key: "owner_username",
                header: "PROFILE",
                render: (row: ProfileRow) =>
                  row.creator_id != null ? (
                    <Link
                      to="/creators/$id"
                      params={{ id: String(row.creator_id) }}
                      className="flex items-center gap-2 hover:opacity-75 transition-opacity"
                      title={`View @${row.owner_username}`}
                    >
                      <Avatar username={row.owner_username} size={24} />
                      <span className="text-cyan-400 font-semibold text-[13px]">
                        {row.owner_username}
                      </span>
                    </Link>
                  ) : (
                    <span className="flex items-center gap-2">
                      <Avatar username={row.owner_username} size={24} />
                      <span className="text-cyan-400 font-semibold text-[13px]">
                        {row.owner_username}
                      </span>
                    </span>
                  ),
              },
              {
                key: "total_posts",
                header: "POSTS",
                className: "text-right font-data tabular-nums",
              },
              {
                key: "enriched_posts",
                header: "ENRICHED",
                className: "text-right font-data tabular-nums",
                render: (row: ProfileRow) => (
                  <span className={row.enriched_posts > 0 ? "text-accent-green" : "text-muted"}>
                    {row.enriched_posts}
                  </span>
                ),
              },
              {
                key: "educational_rate",
                header: "EDU",
                className: "text-right font-data tabular-nums",
                render: (row: ProfileRow) => `${(row.educational_rate * 100).toFixed(0)}%`,
              },
              {
                key: "admiralty_score",
                header: "ADMIRALTY",
                className: "text-center",
                render: (row: ProfileRow) => {
                  const score = row.admiralty_score;
                  let variant: "green" | "yellow" | "orange" | "default" = "default";
                  if (score >= 3) variant = "green";
                  else if (score >= 2) variant = "yellow";
                  else if (score >= 1) variant = "orange";
                  return <Badge variant={variant}>{score.toFixed(1)}</Badge>;
                },
              },
              {
                key: "avg_likes",
                header: "AVG LIKES",
                className: "text-right font-data tabular-nums hidden sm:table-cell",
                render: (row: ProfileRow) => row.avg_likes?.toLocaleString() ?? "--",
              },
              {
                key: "max_likes",
                header: "MAX",
                className: "text-right font-data tabular-nums hidden sm:table-cell",
                render: (row: ProfileRow) => row.max_likes?.toLocaleString() ?? "--",
              },
            ]}
            data={profiles}
            rowKey={(r) => r.owner_id}
            loading={loading}
          />
        </div>
      </Card>
    </div>
  );
}
