import { useEffect, useState, useMemo, useCallback, useRef } from "react";
import { Link, useLocation } from "@tanstack/react-router";
import { AgGridReact } from "ag-grid-react";
import type { CustomHeaderProps } from "ag-grid-react";
import type {
  ColDef,
  GridReadyEvent,
  GridSizeChangedEvent,
  IRowNode,
} from "ag-grid-community";
import {
  AllCommunityModule,
  ModuleRegistry,
} from "ag-grid-community";
import { Avatar } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { FilterModal } from "@/components/ui/filter-modal";
import { PlatformIcon } from "@/components/ui/platform-icon";
import { Info } from "lucide-react";
import { fetchPosts, fetchPostsByProfile, fetchSearchResults, type PostRow } from "@/lib/api";

ModuleRegistry.registerModules([AllCommunityModule]);

// ── Helpers ─────────────────────────────────────────────────────

function admiraltyVariant(
  tier: string | null,
): "green" | "yellow" | "orange" | "default" {
  if (!tier) return "default";
  const t = tier.charAt(0);
  if (t === "A") return "green";
  if (t === "B") return "yellow";
  if (t === "C") return "orange";
  return "default";
}

function formatNumber(n: number | null | undefined): string {
  if (n == null || n === 0) return "--";
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

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

function InfoHeader(props: CustomHeaderProps & { tooltip?: string }) {
  return (
    <div className="flex items-center gap-1.5">
      <span
        className="cursor-pointer select-none"
        title={props.tooltip}
        onClick={() => props.progressSort()}
      >
        {props.displayName}
      </span>
      <Info className="w-3 h-3 text-muted/70 cursor-help shrink-0" aria-hidden />
    </div>
  );
}

// ── Column definitions ──────────────────────────────────────────

const COLUMN_DEFS: ColDef<PostRow>[] = [
  {
    field: "owner_username",
    headerName: "Profile",
    width: 200,
    pinned: "left",
    cellClass: "flex !items-center",
    cellRenderer: ({ data }: { data: PostRow }) =>
      data.creator_id != null ? (
        <Link
          to="/creators/$id"
          params={{ id: String(data.creator_id) }}
          className="flex items-center gap-2 py-1 hover:opacity-75 transition-opacity"
        >
          <Avatar username={data.owner_username} size={24} />
          <span className="text-accent font-semibold text-[13px]">
            {data.owner_username}
          </span>
        </Link>
      ) : (
        <div className="flex items-center gap-2 py-1">
          <Avatar username={data.owner_username} size={24} />
          <span className="text-accent font-semibold text-[13px]">
            {data.owner_username}
          </span>
        </div>
      ),
  },
  {
    field: "platform",
    headerName: "Platform",
    width: 110,
    cellClass: "flex !items-center",
    cellRenderer: ({ data }: { data: PostRow }) => (
      <PlatformIcon platform={data.platform} size={16} />
    ),
  },
  {
    field: "caption",
    headerName: "Caption",
    flex: 2,
    minWidth: 250,
    cellClass: "!block truncate text-[11px] text-muted",
    cellRenderer: ({ value }: { value: string | null }) => {
      if (!value) return <span className="italic text-muted">no caption</span>;
      return <span className="text-muted">{value}</span>;
    },
  },
  {
    field: "likes_count",
    headerName: "Likes",
    width: 90,
    cellClass: "font-data tabular-nums !justify-end",
    valueFormatter: ({ value }: { value: number }) => formatNumber(value),
  },
  {
    field: "comments_count",
    headerName: "Comments",
    width: 100,
    cellClass: "font-data tabular-nums !justify-end",
    valueFormatter: ({ value }: { value: number }) => formatNumber(value),
  },
  {
    field: "video_view_count",
    headerName: "Views",
    width: 90,
    cellClass: "font-data tabular-nums !justify-end",
    valueFormatter: ({ value }: { value: number }) => formatNumber(value),
  },
  {
    field: "admiralty",
    headerName: "Rank",
    width: 80,
    cellClass: "flex !items-center !justify-center",
    cellRenderer: ({ value }: { value: string | null }) => {
      if (!value) return <span className="text-muted text-xs">--</span>;
      return (
        <Badge variant={admiraltyVariant(value)} className="text-[10px]">
          {value}
        </Badge>
      );
    },
  },
  {
    field: "gold_domain",
    headerName: "Domain",
    width: 120,
    cellClass: "flex !items-center",
    cellRenderer: ({ value }: { value: string | null }) =>
      value ? (
        <span className="text-accent-teal text-[12px] font-medium">{value}</span>
      ) : (
        <span className="text-muted text-xs">--</span>
      ),
  },
  {
    field: "gold_topic",
    headerName: "Topic",
    width: 180,
    cellClass: "!block truncate",
    cellRenderer: ({ value }: { value: string | null }) =>
      value ? (
        <span className="text-[12px] text-[#C9D4D4]/70" title={value}>
          {value}
        </span>
      ) : (
        <span className="text-muted text-xs">--</span>
      ),
  },
  {
    field: "is_educational",
    headerName: "EDU",
    width: 80,
    headerComponent: InfoHeader,
    cellClass: "flex !items-center !justify-center",
    headerComponentParams: {
      tooltip: "Educational — the post teaches or informs vs. purely entertaining.",
    },
    cellRenderer: ({ value }: { value: boolean | null }) => {
      if (value === null) return <span className="text-muted text-xs">--</span>;
      return value ? (
        <Badge variant="green">YES</Badge>
      ) : (
        <span className="text-muted text-[11px]">no</span>
      );
    },
  },
  {
    field: "is_actionable",
    headerName: "ACT",
    width: 80,
    headerComponent: InfoHeader,
    cellClass: "flex !items-center !justify-center",
    headerComponentParams: {
      tooltip: "Actionable — the post gives steps or actions the viewer can take.",
    },
    cellRenderer: ({ value }: { value: boolean | null }) => {
      if (value === null) return <span className="text-muted text-xs">--</span>;
      return value ? (
        <Badge variant="accent">YES</Badge>
      ) : (
        <span className="text-muted text-[11px]">no</span>
      );
    },
  },
  {
    field: "timestamp",
    headerName: "Date",
    width: 120,
    cellClass: "font-data text-[11px] text-muted",
    valueFormatter: ({ value }: { value: string | null }) =>
      value ? value.slice(0, 10) : "--",
  },
];

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

  // Load initial data
  useEffect(() => {
    setLoading(true);
    const req = search.username
      ? fetchPostsByProfile(search.username)
      : fetchPosts(0, 0);
    req.then(setData).catch(console.error).finally(() => setLoading(false));
  }, [search.username]);

  // Search handler — debounced server-side full-text search
  useEffect(() => {
    if (searchText.length === 0) {
      const req = search.username
        ? fetchPostsByProfile(search.username)
        : fetchPosts(0, 0);
      req.then(setData).catch(console.error);
      return;
    }
    if (searchText.length < 2) return;

    const timer = setTimeout(() => {
      setLoading(true);
      fetchSearchResults(searchText)
        .then(setData)
        .catch(console.error)
        .finally(() => setLoading(false));
    }, 300);

    return () => clearTimeout(timer);
  }, [searchText, search.username]);

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

  const defaultColDef = useMemo(
    () => ({
      resizable: true,
      sortable: true,
      suppressHeaderMenuButton: true,
    }),
    [],
  );

  const onGridReady = useCallback((params: GridReadyEvent) => {
    params.api.sizeColumnsToFit();
  }, []);

  const onGridSizeChanged = useCallback((params: GridSizeChangedEvent) => {
    params.api.sizeColumnsToFit();
  }, []);

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
      <div className="ag-theme-alpine-dark h-[calc(100vh-200px)] w-full border border-border">
        <AgGridReact<PostRow>
          ref={gridRef}
          onGridSizeChanged={onGridSizeChanged}
          rowData={data}
          columnDefs={COLUMN_DEFS}
          defaultColDef={defaultColDef}
          onGridReady={onGridReady}
          pagination
          paginationPageSize={50}
          paginationPageSizeSelector={[25, 50, 100, 200]}
          quickFilterText={searchText}
          suppressCellFocus
          rowHeight={48}
          headerHeight={36}
          enableCellTextSelection
          ensureDomOrder
          isExternalFilterPresent={isExternalFilterPresent}
          doesExternalFilterPass={doesExternalFilterPass}
          onFilterChanged={onFilterChanged}
        />
      </div>
    </div>
  );
}
