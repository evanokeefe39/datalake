import { useEffect, useState } from "react";
import { Link } from "@tanstack/react-router";
import { Card } from "@/components/ui/card";
import { DataTable } from "@/components/ui/data-table";
import { Badge } from "@/components/ui/badge";
import { Avatar } from "@/components/ui/avatar";
import { PlatformIcon } from "@/components/ui/platform-icon";
import { fetchSignals, type SignalRow } from "@/lib/api";
import { Zap, Info } from "lucide-react";

function InfoHeader({ children, tooltip }: { children: React.ReactNode; tooltip?: string }) {
  return (
    <span className="inline-flex items-center gap-1" title={tooltip}>
      {children}
      <Info className="w-3 h-3 text-muted/70 cursor-help shrink-0" aria-hidden />
    </span>
  );
}

export default function SignalsPage() {
  const [signals, setSignals] = useState<SignalRow[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchSignals()
      .then(setSignals)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <div className="w-8 h-8 border border-accent-yellow flex items-center justify-center">
          <Zap className="w-4 h-4 text-accent-yellow" />
        </div>
        <div>
          <h2 className="text-lg font-bold tracking-[-0.03em] text-white">
            High-Signal Posts
          </h2>
          <p className="text-[11px] text-muted font-data mt-0.5">
            {signals.length} posts with admiralty A or B tier
          </p>
        </div>
      </div>

      <Card className="p-0">
        <DataTable
          columns={[
            {
              key: "owner_username",
              header: "PROFILE",
              sortValue: (row: SignalRow) => row.owner_username,
              render: (row: SignalRow) =>
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
              key: "platform",
              header: "PLATFORM",
              sortValue: (row: SignalRow) => row.platform,
              render: (row: SignalRow) => <PlatformIcon platform={row.platform} size={16} />,
            },
            {
              key: "admiralty",
              header: "RANK",
              className: "text-center",
              sortValue: (row: SignalRow) => row.admiralty,
              render: (row: SignalRow) => {
                const tier = row.admiralty?.charAt(0);
                let variant: "green" | "yellow" | "orange" | "default" =
                  "default";
                if (tier === "A") variant = "green";
                else if (tier === "B") variant = "yellow";
                else variant = "orange";
                return (
                  <Badge variant={variant} className="text-[11px] px-2">
                    {row.admiralty}
                  </Badge>
                );
              },
            },
            {
              key: "gold_domain",
              header: "DOMAIN",
              sortValue: (row: SignalRow) => row.gold_domain,
              render: (row: SignalRow) => (
                <span className="text-accent-magenta text-[13px]">
                  {row.gold_domain}
                </span>
              ),
            },
            {
              key: "gold_topic",
              header: "TOPIC",
              sortValue: (row: SignalRow) => row.gold_topic,
              render: (row: SignalRow) => (
                <span
                  className="text-[13px] text-white/80 truncate block max-w-[260px]"
                  title={row.gold_topic || undefined}
                >
                  {row.gold_topic}
                </span>
              ),
            },
            {
              key: "is_educational",
              header: (
                <InfoHeader tooltip="Educational — the post teaches or informs vs. purely entertaining.">
                  EDU
                </InfoHeader>
              ),
              className: "text-center",
              sortValue: (row: SignalRow) => (row.is_educational ? 1 : 0),
              render: (row: SignalRow) =>
                row.is_educational ? (
                  <Badge variant="green">YES</Badge>
                ) : (
                  <span className="text-muted text-xs">--</span>
                ),
            },
            {
              key: "is_actionable",
              header: (
                <InfoHeader tooltip="Actionable — the post gives steps or actions the viewer can take.">
                  ACT
                </InfoHeader>
              ),
              className: "text-center",
              sortValue: (row: SignalRow) => (row.is_actionable ? 1 : 0),
              render: (row: SignalRow) =>
                row.is_actionable ? (
                  <Badge variant="accent">YES</Badge>
                ) : (
                  <span className="text-muted text-xs">--</span>
                ),
            },
            {
              key: "caption",
              header: "CAPTION",
              sortValue: (row: SignalRow) => row.caption,
              render: (row: SignalRow) => (
                <span
                  className="text-[11px] text-muted truncate block max-w-[240px]"
                  title={row.caption || undefined}
                >
                  {row.caption?.slice(0, 70)}...
                </span>
              ),
            },
          ]}
          data={signals}
          rowKey={(r) => r.post_id}
          loading={loading}
          emptyMessage="No high-signal posts. Enrich more posts to populate this view."
        />
      </Card>
    </div>
  );
}
