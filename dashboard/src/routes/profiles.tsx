import { useEffect, useState } from "react";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Users } from "lucide-react";
import {
  addScrapeTarget,
  fetchScrapeTargets,
  removeScrapeTarget,
  type ScrapeTarget,
} from "@/lib/api";

export default function ProfilesPage() {
  const [targets, setTargets] = useState<ScrapeTarget[]>([]);
  const [loading, setLoading] = useState(true);
  const [username, setUsername] = useState("");
  const [resultsLimit, setResultsLimit] = useState(1);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = () => {
    fetchScrapeTargets()
      .then(setTargets)
      .catch((e: unknown) => setError(String(e)))
      .finally(() => setLoading(false));
  };

  useEffect(refresh, []);

  const handleAdd = async (e: React.FormEvent) => {
    e.preventDefault();
    const name = username.trim();
    if (!name) return;
    setSaving(true);
    setError(null);
    try {
      await addScrapeTarget({
        username: name,
        profile_url: `https://www.instagram.com/${name}/`,
        results_type: "details",
        results_limit: resultsLimit,
        enabled: true,
        tier: "tier1",
      });
      setUsername("");
      refresh();
    } catch (err) {
      setError(String(err));
    } finally {
      setSaving(false);
    }
  };

  const handleRemove = async (name: string) => {
    setError(null);
    try {
      await removeScrapeTarget(name);
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
            Profiles
          </h2>
          <p className="text-[11px] text-muted font-data mt-0.5">
            Tracked profiles — source of truth for scrapes
          </p>
        </div>
      </div>

      <Card className="p-4">
        <form onSubmit={handleAdd} className="flex flex-wrap items-end gap-3">
          <div>
            <label className="block mb-1 text-[10px] text-muted font-data uppercase tracking-widest">
              Username
            </label>
            <input
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="someprofile"
              className="bg-bg border border-border px-3 py-2 text-sm text-white focus:border-accent outline-none"
            />
          </div>
          <div>
            <label className="block mb-1 text-[10px] text-muted font-data uppercase tracking-widest">
              Depth
            </label>
            <input
              type="number"
              min={1}
              value={resultsLimit}
              onChange={(e) => setResultsLimit(Number(e.target.value))}
              className="w-20 bg-bg border border-border px-3 py-2 text-sm text-white focus:border-accent outline-none"
            />
          </div>
          <button
            type="submit"
            disabled={saving || !username.trim()}
            className="border border-accent text-accent px-4 py-2 text-sm hover:bg-accent hover:text-black transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {saving ? "Adding…" : "Add profile"}
          </button>
        </form>
        {error && <p className="text-red-400 text-xs mt-3">{error}</p>}
      </Card>

      <Card className="p-0">
        {loading ? (
          <div className="p-6 text-sm text-muted">Loading…</div>
        ) : targets.length === 0 ? (
          <div className="p-6 text-sm text-muted">
            No profiles tracked yet. Add one above.
          </div>
        ) : (
          <table className="w-full text-left">
            <thead>
              <tr className="border-b border-border text-[10px] text-muted font-data uppercase tracking-widest">
                <th className="px-4 py-3">Profile</th>
                <th className="px-4 py-3">Type</th>
                <th className="px-4 py-3 text-center">Depth</th>
                <th className="px-4 py-3 text-center">Status</th>
                <th className="px-4 py-3" />
              </tr>
            </thead>
            <tbody>
              {targets.map((t) => (
                <tr
                  key={t.username}
                  className="border-b border-border/50 text-sm"
                >
                  <td className="px-4 py-3 text-cyan-400 font-semibold">
                    @{t.username}
                  </td>
                  <td className="px-4 py-3 text-muted">{t.results_type}</td>
                  <td className="px-4 py-3 text-center text-muted">
                    {t.results_limit}
                  </td>
                  <td className="px-4 py-3 text-center">
                    <Badge variant={t.enabled ? "green" : "default"}>
                      {t.enabled ? "enabled" : "disabled"}
                    </Badge>
                  </td>
                  <td className="px-4 py-3 text-right">
                    <button
                      onClick={() => handleRemove(t.username)}
                      className="text-[11px] text-red-400 hover:text-red-300 border border-transparent hover:border-red-400/50 px-2 py-1 transition-colors"
                    >
                      remove
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Card>
    </div>
  );
}
