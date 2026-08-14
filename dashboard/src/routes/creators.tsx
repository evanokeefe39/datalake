import { useEffect, useState } from "react";
import { Link } from "@tanstack/react-router";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { DataTable } from "@/components/ui/data-table";
import { Users } from "lucide-react";
import {
  addCreator,
  fetchCreators,
  removeCreator,
  type Creator,
} from "@/lib/api";

function platformBadge(platform: string) {
  if (platform === "instagram") return "cyan";
  if (platform === "tiktok") return "magenta";
  if (platform === "youtube") return "red";
  return "default";
}

export default function CreatorsPage() {
  const [creators, setCreators] = useState<Creator[]>([]);
  const [loading, setLoading] = useState(true);
  const [name, setName] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = () => {
    fetchCreators()
      .then(setCreators)
      .catch((e: unknown) => setError(String(e)))
      .finally(() => setLoading(false));
  };

  useEffect(refresh, []);

  const handleAdd = async (e: React.FormEvent) => {
    e.preventDefault();
    const creatorName = name.trim();
    if (!creatorName) return;
    setSaving(true);
    setError(null);
    try {
      await addCreator(creatorName);
      setName("");
      refresh();
    } catch (err) {
      setError(String(err));
    } finally {
      setSaving(false);
    }
  };

  const handleRemove = async (id: number) => {
    if (!window.confirm("Remove this creator and all its profiles?")) return;
    setError(null);
    try {
      await removeCreator(id);
      refresh();
    } catch (err) {
      setError(String(err));
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <div className="w-8 h-8 border border-accent flex items-center justify-center">
          <Users className="w-4 h-4 text-accent" />
        </div>
        <div>
          <h2 className="text-lg font-bold tracking-[-0.03em] text-white">
            Creators
          </h2>
          <p className="text-[11px] text-muted font-data mt-0.5">
            People and brands — each owns one or more profiles
          </p>
        </div>
      </div>

      <Card className="p-4">
        <form onSubmit={handleAdd} className="flex flex-wrap items-end gap-3">
          <div>
            <label className="block mb-1 text-[10px] text-muted font-data uppercase tracking-widest">
              Creator name
            </label>
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Jane Doe"
              className="bg-bg border border-border px-3 py-2 text-sm text-white focus:border-accent outline-none"
            />
          </div>
          <button
            type="submit"
            disabled={saving || !name.trim()}
            className="border border-accent text-accent px-4 py-2 text-sm hover:bg-accent hover:text-black transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {saving ? "Adding…" : "Add creator"}
          </button>
        </form>
        {error && <p className="text-red-400 text-xs mt-3">{error}</p>}
      </Card>

      <DataTable
        columns={[
          {
            key: "name",
            header: "Creator",
            render: (row: Creator) => (
              <Link
                to="/creators/$id"
                params={{ id: String(row.id) }}
                className="text-cyan-400 font-semibold hover:opacity-75 transition-opacity"
              >
                {row.name}
              </Link>
            ),
          },
          {
            key: "profile_count",
            header: "Profiles",
            className: "text-center",
            render: (row: Creator) => (
              <span className="text-muted">{row.profile_count}</span>
            ),
          },
          {
            key: "platforms",
            header: "Platforms",
            render: (row: Creator) => (
              <span className="flex gap-1.5 flex-wrap">
                {row.platforms.length === 0 ? (
                  <span className="text-muted">—</span>
                ) : (
                  row.platforms.map((p) => (
                    <Badge key={p} variant={platformBadge(p)}>
                      {p}
                    </Badge>
                  ))
                )}
              </span>
            ),
          },
          {
            key: "total_posts",
            header: "Posts",
            className: "text-center",
            render: (row: Creator) => (
              <span className="text-muted">{row.total_posts}</span>
            ),
          },
          {
            key: "actions",
            header: "",
            className: "text-right",
            render: (row: Creator) => (
              <button
                onClick={() => handleRemove(row.id)}
                className="text-[11px] text-red-400 hover:text-red-300 border border-transparent hover:border-red-400/50 px-2 py-1 transition-colors"
              >
                remove
              </button>
            ),
          },
        ]}
        data={creators}
        rowKey={(r) => String(r.id)}
        loading={loading}
        emptyMessage="No creators yet. Add one above."
      />
    </div>
  );
}
