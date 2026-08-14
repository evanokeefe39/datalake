import { useEffect, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { PLATFORMS, platformBadge, normalizeHandle } from "@/lib/platform";
import {
  addCreator,
  addProfile,
  editDepth,
  removeCreator,
  removeProfile,
  renameCreator,
  type CreatorDetail,
} from "@/lib/api";

interface ModalProfile {
  platform: string;
  handle: string;
  results_limit: number;
  /** True when seeded from an existing creator profile (edit mode). */
  existing: boolean;
}

interface CreatorModalProps {
  open: boolean;
  onClose: () => void;
  onSaved: () => void;
  /** When set, the modal runs in edit mode for this creator. */
  creator?: CreatorDetail;
}

const keyOf = (p: { platform: string; handle: string }) => `${p.platform}:${p.handle}`;

export function CreatorModal({ open, onClose, onSaved, creator }: CreatorModalProps) {
  const isEdit = creator != null;

  const [name, setName] = useState("");
  const [profiles, setProfiles] = useState<ModalProfile[]>([]);
  const [platform, setPlatform] = useState<string>("instagram");
  const [handle, setHandle] = useState("");
  const [depth, setDepth] = useState(1);
  const [batchText, setBatchText] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState(false);

  // Reset the form each time the modal opens (or its subject changes).
  useEffect(() => {
    if (!open) return;
    setName(creator?.name ?? "");
    setProfiles(
      creator
        ? creator.profiles.map((p) => ({
            platform: p.platform,
            handle: p.handle,
            results_limit: p.results_limit,
            existing: true,
          }))
        : [],
    );
    setPlatform("instagram");
    setHandle("");
    setDepth(1);
    setBatchText("");
    setError(null);
    setConfirmDelete(false);
  }, [open, creator]);

  // Close on Escape.
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  if (!open) return null;

  const addToProfiles = (items: { platform: string; handle: string }[]) => {
    setProfiles((prev) => {
      const seen = new Set(prev.map(keyOf));
      const additions: ModalProfile[] = [];
      for (const it of items) {
        const k = keyOf(it);
        if (seen.has(k)) continue;
        seen.add(k);
        additions.push({ ...it, results_limit: depth, existing: false });
      }
      return [...prev, ...additions];
    });
  };

  const handleAddSingle = (e: React.FormEvent) => {
    e.preventDefault();
    const h = normalizeHandle(handle);
    if (!h) return;
    addToProfiles([{ platform, handle: h }]);
    setHandle("");
  };

  const handleAddBatch = (e: React.FormEvent) => {
    e.preventDefault();
    const handles = batchText
      .split(/[\n,]+/)
      .map(normalizeHandle)
      .filter(Boolean);
    if (handles.length === 0) return;
    addToProfiles(handles.map((h) => ({ platform, handle: h })));
    setBatchText("");
  };

  const handleDepth = (p: ModalProfile, value: number) => {
    if (value < 1) return;
    setProfiles((prev) =>
      prev.map((x) => (keyOf(x) === keyOf(p) ? { ...x, results_limit: value } : x)),
    );
  };

  const handleRemoveProfile = (p: ModalProfile) => {
    setProfiles((prev) => prev.filter((x) => keyOf(x) !== keyOf(p)));
  };

  const handleSave = async () => {
    const trimmed = name.trim();
    if (!trimmed) return;
    setSaving(true);
    setError(null);
    try {
      if (!isEdit) {
        const created = await addCreator(trimmed);
        for (const p of profiles) {
          await addProfile(created.id, {
            platform: p.platform,
            handle: p.handle,
            results_limit: p.results_limit,
            enabled: true,
            tier: "tier1",
          });
        }
      } else if (creator) {
        if (trimmed !== creator.name) {
          await renameCreator(creator.id, trimmed);
        }
        const original = new Map(creator.profiles.map((p) => [keyOf(p), p]));
        const currentKeys = new Set(profiles.map(keyOf));
        for (const [k, p] of original) {
          if (!currentKeys.has(k)) await removeProfile(p.platform, p.handle);
        }
        for (const p of profiles) {
          const orig = original.get(keyOf(p));
          if (!orig) {
            await addProfile(creator.id, {
              platform: p.platform,
              handle: p.handle,
              results_limit: p.results_limit,
              enabled: true,
              tier: "tier1",
            });
          } else if (orig.results_limit !== p.results_limit) {
            await editDepth(p.platform, p.handle, p.results_limit);
          }
        }
      }
      onSaved();
      onClose();
    } catch (err) {
      setError(String(err));
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!creator) return;
    setSaving(true);
    setError(null);
    try {
      await removeCreator(creator.id);
      onSaved();
      onClose();
    } catch (err) {
      setError(String(err));
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div
        className="absolute inset-0 bg-black/70"
        onClick={() => {
          if (!saving) onClose();
        }}
      />
      <div className="relative w-full max-w-lg max-h-[85vh] overflow-y-auto bg-surface border border-border p-6">
        <h2 className="text-base font-bold tracking-[-0.02em] text-white">
          {isEdit ? "Edit creator" : "Add creator"}
        </h2>

        <label className="block mt-5 mb-1 text-[10px] text-muted font-data uppercase tracking-widest">
          Creator name
        </label>
        <input
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="Jane Doe"
          className="w-full bg-bg border border-border px-3 py-2 text-sm text-white focus:border-accent outline-none"
        />

        <h3 className="mt-6 mb-2 text-[10px] text-muted font-data uppercase tracking-widest">
          Profiles
        </h3>
        {profiles.length === 0 ? (
          <p className="text-xs text-muted py-2">No profiles yet.</p>
        ) : (
          <ul className="space-y-2">
            {profiles.map((p) => (
              <li
                key={keyOf(p)}
                className="flex items-center gap-2 border border-border bg-bg px-3 py-2"
              >
                <Badge variant={platformBadge(p.platform)}>{p.platform}</Badge>
                <span className="flex-1 min-w-0 truncate text-cyan-400 font-semibold text-sm">
                  @{p.handle}
                </span>
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
                  className="text-muted hover:text-red-400 px-1"
                  aria-label={`Remove @${p.handle}`}
                >
                  ✕
                </button>
              </li>
            ))}
          </ul>
        )}

        <form onSubmit={handleAddSingle} className="mt-4 flex gap-2">
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
            className="flex-1 min-w-0 bg-bg border border-border px-3 py-2 text-sm text-white focus:border-accent outline-none"
          />
          <input
            type="number"
            min={1}
            value={depth}
            onChange={(e) => setDepth(Number(e.target.value))}
            className="w-16 bg-bg border border-border px-2 py-2 text-sm text-white focus:border-accent outline-none text-center"
            aria-label="Depth"
          />
          <button
            type="submit"
            disabled={!handle.trim()}
            className="border border-accent text-accent px-3 py-2 text-sm hover:bg-accent hover:text-black transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
          >
            Add
          </button>
        </form>

        <form onSubmit={handleAddBatch} className="mt-3 space-y-2">
          <textarea
            value={batchText}
            onChange={(e) => setBatchText(e.target.value)}
            placeholder="one handle per line, or comma-separated"
            rows={2}
            className="w-full bg-bg border border-border px-3 py-2 text-sm text-white focus:border-accent outline-none resize-none"
          />
          <button
            type="submit"
            disabled={!batchText.trim()}
            className="border border-accent text-accent px-3 py-2 text-sm hover:bg-accent hover:text-black transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
          >
            Add all
          </button>
        </form>

        {error && <p className="text-red-400 text-xs mt-4">{error}</p>}

        <div className="mt-6 pt-4 border-t border-border flex items-center justify-between gap-3">
          {isEdit ? (
            confirmDelete ? (
              <div className="flex items-center gap-2">
                <span className="text-[11px] text-red-400">Delete creator + profiles?</span>
                <button
                  onClick={handleDelete}
                  disabled={saving}
                  className="border border-red-400 text-red-400 px-2 py-1 text-[11px] hover:bg-red-400 hover:text-black transition-colors disabled:opacity-40"
                >
                  Delete
                </button>
                <button
                  onClick={() => setConfirmDelete(false)}
                  disabled={saving}
                  className="text-[11px] text-muted hover:text-white"
                >
                  Cancel
                </button>
              </div>
            ) : (
              <button
                onClick={() => setConfirmDelete(true)}
                disabled={saving}
                className="text-[11px] text-red-400 hover:text-red-300"
              >
                Delete creator
              </button>
            )
          ) : (
            <span />
          )}

          <div className="flex items-center gap-2">
            <button
              onClick={onClose}
              disabled={saving}
              className="text-sm text-muted hover:text-white px-3 py-2 transition-colors"
            >
              Cancel
            </button>
            <button
              onClick={handleSave}
              disabled={saving || !name.trim()}
              className="border border-accent text-accent px-4 py-2 text-sm hover:bg-accent hover:text-black transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              {saving ? "Saving…" : "Save"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
