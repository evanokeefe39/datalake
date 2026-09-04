import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useLocation } from "@tanstack/react-router";
import { AgGridReact } from "ag-grid-react";
import type { IRowNode } from "ag-grid-community";
import { FilterModal } from "@/components/ui/filter-modal";
import { PostsTable } from "@/components/ui/posts-table";
import { fetchPosts, fetchPostsByProfile, fetchSearchResults, type PostRow } from "@/lib/api";

// ── Filter panel state ──────────────────────────────────────────

interface AdvancedFilters {
  platforms: Set<string>;
  domains: Set<string>;
  ranks: Set<string>;
  educational: "all" | "true" | "false";
  actionable: "all" | "true" | "false";
  minLikes: number | null;
  maxLikes: number | null;
  minComments: number | null;
  maxComments: number | null;
  minViews: number | null;
  maxViews: number | null;
  dateFrom: string | null;
  dateTo: string | null;
}

const emptyFilters = (): AdvancedFilters => ({
  platforms: new Set(),
  domains: new Set(),
  ranks: new Set(),
  educational: "all",
  actionable: "all",
  minLikes: null,
  maxLikes: null,
  minComments: null,
  maxComments: null,
  minViews: null,
  maxViews: null,
  dateFrom: null,
  dateTo: null,
});

function filtersActive(f: AdvancedFilters): boolean {
  return (
    f.platforms.size > 0 ||
    f.domains.size > 0 ||
    f.ranks.size > 0 ||
    f.educational !== "all" ||
    f.actionable !== "all" ||
    f.minLikes !== null ||
    f.maxLikes !== null ||
    f.minComments !== null ||
    f.maxComments !== null ||
    f.minViews !== null ||
    f.maxViews !== null ||
    f.dateFrom !== null ||
    f.dateTo !== null
  );
}

function toggleSet<T>(set: Set<T>, item: T): Set<T> {
  const next = new Set(set);
  if (next.has(item)) next.delete(item);
  else next.add(item);
  return next;
}

const PLATFORM_OPTIONS = ["instagram", "tiktok", "youtube", "reddit", "twitter"] as const;
const DOMAIN_OPTIONS = ["Tech", "Business", "Creative", "Lifestyle", "Education"] as const;
const RANK_OPTIONS = ["A", "B", "C"] as const;


// ── Component ───────────────────────────────────────────────────

export default function PostsPage() {
  const [data, setData] = useState<PostRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchText, setSearchText] = useState("");
  const [showFilters, setShowFilters] = useState(false);
  const [filters, setFilters] = useState<AdvancedFilters>(emptyFilters);
  const gridRef = useRef<AgGridReact<PostRow>>(null);
  const location = useLocation();
  const search = location.search as { username?: string };

  // Single loader: fetch the full working set when the page loads, the profile
  // changes, or the quick search is cleared (< 2 chars). No duplicate mount fetch.
  useEffect(() => {
    if (searchText.length >= 2) return;
    let cancelled = false;
    setLoading(true);
    const req = search.username
      ? fetchPostsByProfile(search.username)
      : fetchPosts(0, 0);
    req
      .then((rows) => {
        if (!cancelled) setData(rows);
      })
      .catch(console.error)
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [searchText, search.username]);

  // Debounced server-side full-text search once the query is >= 2 chars.
  useEffect(() => {
    if (searchText.length < 2) return;
    const timer = setTimeout(() => {
      setLoading(true);
      fetchSearchResults(searchText)
        .then(setData)
        .catch(console.error)
        .finally(() => setLoading(false));
    }, 300);
    return () => clearTimeout(timer);
  }, [searchText]);

  // Apply external filters when the Filter modal criteria change
  const isExternalFilterPresent = useCallback((): boolean => {
    return filtersActive(filters);
  }, [filters]);

  const doesExternalFilterPass = useCallback(
    (node: IRowNode<PostRow>): boolean => {
      const row = node.data;
      if (!row) return false;
      if (filters.platforms.size > 0 && !filters.platforms.has(row.platform ?? ""))
        return false;
      if (filters.domains.size > 0 && !filters.domains.has(row.gold_domain ?? ""))
        return false;
      if (filters.ranks.size > 0) {
        const rank = (row.admiralty ?? "")[0];
        if (!filters.ranks.has(rank)) return false;
      }
      if (filters.educational === "true" && row.is_educational !== true) return false;
      if (filters.educational === "false" && row.is_educational === true) return false;
      if (filters.actionable === "true" && row.is_actionable !== true) return false;
      if (filters.actionable === "false" && row.is_actionable === true) return false;
      if (filters.minLikes !== null && (row.likes_count ?? 0) < filters.minLikes)
        return false;
      if (filters.maxLikes !== null && (row.likes_count ?? 0) > filters.maxLikes)
        return false;
      if (filters.minComments !== null && (row.comments_count ?? 0) < filters.minComments)
        return false;
      if (filters.maxComments !== null && (row.comments_count ?? 0) > filters.maxComments)
        return false;
      if (filters.minViews !== null && (row.video_view_count ?? 0) < filters.minViews)
        return false;
      if (filters.maxViews !== null && (row.video_view_count ?? 0) > filters.maxViews)
        return false;
      if (filters.dateFrom !== null && row.timestamp && row.timestamp < filters.dateFrom)
        return false;
      if (filters.dateTo !== null && row.timestamp && row.timestamp > filters.dateTo)
        return false;
      return true;
    },
    [filters],
  );

  const onFilterChanged = useCallback(() => {
    gridRef.current?.api.onFilterChanged();
  }, []);

  const clearFilters = () => setFilters(emptyFilters());


  if (loading && data.length === 0) {
    return (
      <div className="flex items-center justify-center h-64 text-muted font-data text-xs tracking-widest animate-pulse">
        LOADING POSTS
      </div>
    );
  }

  const rangeInputClass =
    "w-full bg-[#100e0e] border border-border text-[#C9D4D4] text-[12px] px-2 py-1.5 placeholder:text-muted font-data focus:border-accent focus:outline-none";

  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-lg font-bold tracking-[-0.03em] text-[#C9D4D4]">Posts</h2>
        <p className="text-[11px] text-muted font-data mt-0.5">
          {data.length.toLocaleString()} posts
          {filtersActive(filters) ? " · filters active" : ""}
          {" · click column headers to sort"}
        </p>
        {search.username && (
          <div className="mt-2 flex items-center gap-2">
            <span className="text-[11px] text-accent font-data tracking-widest">
              FILTERED: @{search.username}
            </span>
            <Link to="/posts" className="text-[11px] text-muted hover:text-white underline">
              clear
            </Link>
          </div>
        )}
      </div>

      {/* ── Search bar + Filter button ──────────────────────────── */}
      <div className="flex items-center gap-3">
        <div className="relative flex-1 max-w-md">
          <svg
            className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
            />
          </svg>
          <input
            type="text"
            placeholder="Search captions, topics, usernames..."
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            className="w-full bg-[#1c1818] border border-border text-[#C9D4D4] text-[13px]
                       pl-10 pr-4 py-2 placeholder:text-muted
                       focus:border-accent focus:outline-none transition-colors
                       font-data"
            aria-label="Search posts"
          />
          {searchText.length > 0 && (
            <button
              onClick={() => setSearchText("")}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-muted hover:text-accent transition-colors"
              aria-label="Clear search"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          )}
        </div>

        <button
          onClick={() => setShowFilters(true)}
          className={`px-3 py-2 text-[11px] font-data border transition-colors flex items-center gap-2
            ${filtersActive(filters)
              ? "border-accent text-accent bg-[#1c1818]"
              : "border-border text-muted hover:border-accent-dim hover:text-[#C9D4D4]"
            }`}
          aria-label="Open filters"
        >
          <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
          </svg>
          Filter
          {filtersActive(filters) && (
            <span className="w-2 h-2 rounded-full bg-accent inline-block" />
          )}
        </button>
      </div>

      {/* ── Filter modal ────────────────────────────────────────── */}
      <FilterModal
        open={showFilters}
        onClose={() => setShowFilters(false)}
        title="Filters"
      >
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <span className="text-[11px] font-data text-muted uppercase tracking-widest">
              Filter Criteria
            </span>
            <button
              onClick={clearFilters}
              className="text-[10px] font-data text-muted hover:text-accent transition-colors"
            >
              Clear All
            </button>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            {/* Platform checkboxes */}
            <fieldset>
              <legend className="text-[10px] font-data text-muted uppercase mb-2">Platform</legend>
              <div className="space-y-1">
                {PLATFORM_OPTIONS.map((p) => (
                  <label
                    key={p}
                    className="flex items-center gap-2 text-[12px] text-[#C9D4D4] cursor-pointer font-data capitalize"
                  >
                    <input
                      type="checkbox"
                      checked={filters.platforms.has(p)}
                      onChange={() =>
                        setFilters((f) => ({ ...f, platforms: toggleSet(f.platforms, p) }))
                      }
                      style={{ accentColor: "#E2BDB1" }}
                    />
                    {p}
                  </label>
                ))}
              </div>
            </fieldset>

            {/* Domain checkboxes */}
            <fieldset>
              <legend className="text-[10px] font-data text-muted uppercase mb-2">Domain</legend>
              <div className="space-y-1">
                {DOMAIN_OPTIONS.map((d) => (
                  <label
                    key={d}
                    className="flex items-center gap-2 text-[12px] text-[#C9D4D4] cursor-pointer font-data"
                  >
                    <input
                      type="checkbox"
                      checked={filters.domains.has(d)}
                      onChange={() =>
                        setFilters((f) => ({ ...f, domains: toggleSet(f.domains, d) }))
                      }
                      style={{ accentColor: "#E2BDB1" }}
                    />
                    {d}
                  </label>
                ))}
              </div>
            </fieldset>

            {/* Rank checkboxes */}
            <fieldset>
              <legend className="text-[10px] font-data text-muted uppercase mb-2">Rank Tier</legend>
              <div className="space-y-1">
                {RANK_OPTIONS.map((r) => (
                  <label
                    key={r}
                    className="flex items-center gap-2 text-[12px] text-[#C9D4D4] cursor-pointer font-data"
                  >
                    <input
                      type="checkbox"
                      checked={filters.ranks.has(r)}
                      onChange={() =>
                        setFilters((f) => ({ ...f, ranks: toggleSet(f.ranks, r) }))
                      }
                      style={{ accentColor: "#E2BDB1" }}
                    />
                    Tier {r}
                  </label>
                ))}
              </div>
            </fieldset>

            {/* EDU / ACT selects */}
            <fieldset className="grid grid-cols-2 gap-3">
              <div>
                <legend className="text-[10px] font-data text-muted uppercase mb-2">EDU</legend>
                <select
                  value={filters.educational}
                  onChange={(e) =>
                    setFilters((f) => ({
                      ...f,
                      educational: e.target.value as AdvancedFilters["educational"],
                    }))
                  }
                  className={`${rangeInputClass} bg-[#100e0e]`}
                  aria-label="Educational filter"
                >
                  <option value="all">All</option>
                  <option value="true">Yes</option>
                  <option value="false">No</option>
                </select>
              </div>
              <div>
                <legend className="text-[10px] font-data text-muted uppercase mb-2">ACT</legend>
                <select
                  value={filters.actionable}
                  onChange={(e) =>
                    setFilters((f) => ({
                      ...f,
                      actionable: e.target.value as AdvancedFilters["actionable"],
                    }))
                  }
                  className={`${rangeInputClass} bg-[#100e0e]`}
                  aria-label="Actionable filter"
                >
                  <option value="all">All</option>
                  <option value="true">Yes</option>
                  <option value="false">No</option>
                </select>
              </div>
            </fieldset>

            {/* Likes range */}
            <fieldset>
              <legend className="text-[10px] font-data text-muted uppercase mb-2">
                Likes Range
              </legend>
              <div className="flex items-center gap-2">
                <input
                  type="number"
                  placeholder="Min"
                  value={filters.minLikes ?? ""}
                  onChange={(e) =>
                    setFilters((f) => ({
                      ...f,
                      minLikes: e.target.value ? Number(e.target.value) : null,
                    }))
                  }
                  className={rangeInputClass}
                  aria-label="Min likes"
                />
                <span className="text-muted text-xs">–</span>
                <input
                  type="number"
                  placeholder="Max"
                  value={filters.maxLikes ?? ""}
                  onChange={(e) =>
                    setFilters((f) => ({
                      ...f,
                      maxLikes: e.target.value ? Number(e.target.value) : null,
                    }))
                  }
                  className={rangeInputClass}
                  aria-label="Max likes"
                />
              </div>
            </fieldset>

            {/* Comments range */}
            <fieldset>
              <legend className="text-[10px] font-data text-muted uppercase mb-2">
                Comments Range
              </legend>
              <div className="flex items-center gap-2">
                <input
                  type="number"
                  placeholder="Min"
                  value={filters.minComments ?? ""}
                  onChange={(e) =>
                    setFilters((f) => ({
                      ...f,
                      minComments: e.target.value ? Number(e.target.value) : null,
                    }))
                  }
                  className={rangeInputClass}
                  aria-label="Min comments"
                />
                <span className="text-muted text-xs">–</span>
                <input
                  type="number"
                  placeholder="Max"
                  value={filters.maxComments ?? ""}
                  onChange={(e) =>
                    setFilters((f) => ({
                      ...f,
                      maxComments: e.target.value ? Number(e.target.value) : null,
                    }))
                  }
                  className={rangeInputClass}
                  aria-label="Max comments"
                />
              </div>
            </fieldset>

            {/* Views range */}
            <fieldset>
              <legend className="text-[10px] font-data text-muted uppercase mb-2">
                Views Range
              </legend>
              <div className="flex items-center gap-2">
                <input
                  type="number"
                  placeholder="Min"
                  value={filters.minViews ?? ""}
                  onChange={(e) =>
                    setFilters((f) => ({
                      ...f,
                      minViews: e.target.value ? Number(e.target.value) : null,
                    }))
                  }
                  className={rangeInputClass}
                  aria-label="Min views"
                />
                <span className="text-muted text-xs">–</span>
                <input
                  type="number"
                  placeholder="Max"
                  value={filters.maxViews ?? ""}
                  onChange={(e) =>
                    setFilters((f) => ({
                      ...f,
                      maxViews: e.target.value ? Number(e.target.value) : null,
                    }))
                  }
                  className={rangeInputClass}
                  aria-label="Max views"
                />
              </div>
            </fieldset>

            {/* Date range */}
            <fieldset>
              <legend className="text-[10px] font-data text-muted uppercase mb-2">
                Date Range
              </legend>
              <div className="space-y-2">
                <input
                  type="date"
                  value={filters.dateFrom ?? ""}
                  onChange={(e) =>
                    setFilters((f) => ({ ...f, dateFrom: e.target.value || null }))
                  }
                  className={`${rangeInputClass} bg-[#100e0e]`}
                  aria-label="Date from"
                  style={{ colorScheme: "dark" }}
                />
                <input
                  type="date"
                  value={filters.dateTo ?? ""}
                  onChange={(e) =>
                    setFilters((f) => ({ ...f, dateTo: e.target.value || null }))
                  }
                  className={`${rangeInputClass} bg-[#100e0e]`}
                  aria-label="Date to"
                  style={{ colorScheme: "dark" }}
                />
              </div>
            </fieldset>
          </div>
        </div>
      </FilterModal>

      {/* ── Grid ────────────────────────────────────────────────── */}
      <PostsTable
        rows={data}
        quickFilterText={searchText}
        pagination
        gridRef={gridRef}
        isExternalFilterPresent={isExternalFilterPresent}
        doesExternalFilterPass={doesExternalFilterPass}
        onFilterChanged={onFilterChanged}
      />
    </div>
  );
}
