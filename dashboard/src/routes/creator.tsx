import { useEffect, useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Avatar } from "@/components/ui/avatar";
import { Users, ArrowLeft } from "lucide-react";
import {
  addProfile,
  addProfilesBatch,
  editDepth,
  fetchCreator,
  removeProfile,
  renameCreator,
  type CreatorDetail,
  type CreatorProfile,
} from "@/lib/api";

const PLATFORMS = ["instagram", "tiktok", "youtube"] as const;

function platformBadge(platform: string) {
  if (platform === "instagram") return "cyan";
  if (platform === "tiktok") return "magenta";
  if (platform === "youtube") return "red";
  return "default";
}

export default function CreatorPage() {
  const { id } = useParams({ from: "/creators/$id" });
  const creatorId = Number(id);

  const [creator, setCreator] = useState<CreatorDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [renameValue, setRenameValue] = useState("");
  const [platform, setPlatform] = useState<string>("instagram");
  const [handle, setHandle] = useState("");
  const [depth, setDepth] = useState(1);
  const [batchHandles, setBatchHandles] = useState("");
  const [saving, setSaving] = useState(false);

  const refresh = () => {
    fetchCreator(creatorId)
      .then((c) => {
        setCreator(c);
        setRenameValue(c.name);
      })
      .catch((e: unknown) => setError(String(e)))
      .finally(() => setLoading(false));
  };

  useEffect(refresh, [creatorId]);

  const run = async (fn: () => Promise<unknown>) => {
    setSaving(true);
    setError(null);
    try {
      await fn();
      refresh();
    } catch (err) {
      setError(String(err));
    } finally {
      setSaving(false);
    }
  };

  const handleRename = (e: React.FormEvent) => {
    e.preventDefault();
    if (!renameValue.trim()) return;
    run(() => renameCreator(creatorId, renameValue.trim()));
  };

  const handleAddProfile = (e: React.FormEvent) => {
    e.preventDefault();
    if (!handle.trim()) return;
    run(() =>
      addProfile(creatorId, {
        platform,
        handle: handle.trim(),
        results_limit: depth,
        enabled: true,
        tier: "tier1",
      }).then(() => setHandle("")),
    );
  };

  const handleBatch = (e: React.FormEvent) => {
    e.preventDefault();
    const handles = batchHandles
      .split(/[\n,]+/)
      .map((h) => h.trim())
      .filter(Boolean);
    if (handles.length === 0) return;
    run(() =>
      addProfilesBatch(creatorId, {
        platform,
        handles,
        results_limit: depth,
        enabled: true,
        tier: "tier1",
      }).then(() => setBatchHandles("")),
    );
  };

  const handleDepth = (p: CreatorProfile, value: number) => {
    if (value < 1) return;
    run(() => editDepth(p.platform, p.handle, value));
  };

  const handleRemoveProfile = (p: CreatorProfile) => {
    if (!window.confirm(`Remove @${p.handle}?`)) return;
    run(() => removeProfile(p.platform, p.handle));
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

  const avatarHandle = creator.profiles.find((p) => p.platform === "instagram")?.handle;

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
        <div className="flex-1">
          <form onSubmit={handleRename} className="flex items-center gap-2">
            <input
              value={renameValue}
              onChange={(e) => setRenameValue(e.target.value)}
              className="bg-transparent text-xl font-bold tracking-[-0.03em] text-white border border-transparent focus:border-accent px-2 py-1 -ml-2 outline-none w-full max-w-md"
              aria-label="Creator name"
            />
            <button
              type="submit"
              disabled={saving || !renameValue.trim() || renameValue.trim() === creator.name}
              className="text-[11px] text-accent border border-accent px-2 py-1 hover:bg-accent hover:text-black transition-colors disabled:opacity-40"
            >
              rename
            </button>
          </form>
          <p className="text-[11px] text-muted font-data mt-1">
            {creator.profiles.length} profile{creator.profiles.length === 1 ? "" : "s"}
          </p>
        </div>
      </div>

      {error && <p className="text-red-400 text-xs">{error}</p>}

      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="p-4">
          <h3 className="text-[10px] text-muted font-data uppercase tracking-widest mb-3">
            Add profile
          </h3>
          <form onSubmit={handleAddProfile} className="space-y-3">
            <div className="flex gap-2">
              <select
                value={platform}
                onChange={(e) => setPlatform(e.target.value)}
                className="bg-bg border border-border px-2 py-2 text-sm text-white focus:border-accent outline-none"
              >
                {PLATFORMS.map((p) => (
                  <option key={p} value={p}>
                    {p}
                  </option>
                ))}
              </select>
              <input
                value={handle}
                onChange={(e) => setHandle(e.target.value)}
                placeholder="handle or URL"
                className="flex-1 bg-bg border border-border px-3 py-2 text-sm text-white focus:border-accent outline-none"
              />
              <input
                type="number"
                min={1}
                value={depth}
                onChange={(e) => setDepth(Number(e.target.value))}
                className="w-20 bg-bg border border-border px-3 py-2 text-sm text-white focus:border-accent outline-none"
                aria-label="Depth"
              />
            </div>
            <button
              type="submit"
              disabled={saving || !handle.trim()}
              className="border border-accent text-accent px-4 py-2 text-sm hover:bg-accent hover:text-black transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              Add profile
            </button>
          </form>
        </Card>

        <Card className="p-4">
          <h3 className="text-[10px] text-muted font-data uppercase tracking-widest mb-3">
            Batch add
          </h3>
          <form onSubmit={handleBatch} className="space-y-3">
            <textarea
              value={batchHandles}
              onChange={(e) => setBatchHandles(e.target.value)}
              placeholder={"one handle per line, or comma-separated"}
              rows={3}
              className="w-full bg-bg border border-border px-3 py-2 text-sm text-white focus:border-accent outline-none resize-none"
            />
            <button
              type="submit"
              disabled={saving || !batchHandles.trim()}
              className="border border-accent text-accent px-4 py-2 text-sm hover:bg-accent hover:text-black transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              Add all
            </button>
          </form>
        </Card>
      </div>

      <div className="space-y-3">
        {creator.profiles.length === 0 ? (
          <Card className="p-6 text-sm text-muted text-center">
            No profiles attached. Add one above.
          </Card>
        ) : (
          creator.profiles.map((p) => (
            <Card key={`${p.platform}:${p.handle}`} className="p-4">
              <div className="flex items-center gap-3">
                <Avatar username={p.handle} size={36} />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <Badge variant={platformBadge(p.platform)}>{p.platform}</Badge>
                    <span className="text-cyan-400 font-semibold text-sm truncate">
                      @{p.handle}
                    </span>
                  </div>
                  <p className="text-[11px] text-muted font-data mt-0.5">
                    {p.platform === "instagram" ? (
                      <>
                        {p.full_name ? `${p.full_name} · ` : ""}
                        {p.followers_count ? `${p.followers_count.toLocaleString()} followers · ` : ""}
                        {p.post_count} posts held
                      </>
                    ) : (
                      "not scraped"
                    )}
                  </p>
                </div>
                <div className="flex items-center gap-3">
                  <div className="flex items-center gap-1">
                    <span className="text-[10px] text-muted font-data uppercase tracking-widest">
                      depth
                    </span>
                    <input
                      type="number"
                      min={1}
                      value={p.results_limit}
                      onChange={(e) => handleDepth(p, Number(e.target.value))}
                      className="w-16 bg-bg border border-border px-2 py-1 text-sm text-white focus:border-accent outline-none text-center"
                    />
                  </div>
                  <button
                    onClick={() => handleRemoveProfile(p)}
                    className="text-[11px] text-red-400 hover:text-red-300 border border-transparent hover:border-red-400/50 px-2 py-1 transition-colors"
                  >
                    remove
                  </button>
                </div>
              </div>
            </Card>
          ))
        )}
      </div>
    </div>
  );
}
