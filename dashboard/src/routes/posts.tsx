import { useEffect, useState, useMemo, useCallback, useRef } from "react";
import { Link, useLocation } from "@tanstack/react-router";
import { AgGridReact } from "ag-grid-react";
import type { CustomHeaderProps } from "ag-grid-react";
import type {
  ColDef,
  GridReadyEvent,
  GridSizeChangedEvent,
  IDoesFilterPassParams,
  IFilterParams,
} from "ag-grid-community";
import {
  AllCommunityModule,
  ModuleRegistry,
} from "ag-grid-community";
import { Avatar } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
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

// ── Custom Boolean Filter Component ─────────────────────────────

interface BooleanFilterState {
  value: "all" | "true" | "false";
}

class BooleanFilter {
  private eGui!: HTMLElement;
  private value: "all" | "true" | "false" = "all";
  private filterChangedCallback!: (additionalEventAttributes?: unknown) => void;
  private params!: IFilterParams;

  init(params: IFilterParams) {
    this.params = params;
    this.eGui = document.createElement("div");
    this.eGui.className = "boolean-filter";
    this.render();
  }

  getGui() {
    return this.eGui;
  }

  isFilterActive() {
    return this.value !== "all";
  }

  doesFilterPass(params: IDoesFilterPassParams) {
    const val = this.params.getValue(params.node);
    if (this.value === "true") return val === true;
    if (this.value === "false") return val === false || val === null;
    return true;
  }

  getModel(): BooleanFilterState | null {
    return this.isFilterActive() ? { value: this.value } : null;
  }

  setModel(model: BooleanFilterState | null) {
    this.value = model?.value ?? "all";
    this.render();
    this.filterChangedCallback({});
  }

  onNewRowsLoaded() {
    // no-op — state is self-contained
  }

  destroy() {
    // no-op
  }

  // @ts-expect-error AG Grid custom filter callback
  registerOnFilterChangedCallback(cb: () => void) {
    this.filterChangedCallback = cb;
  }

  private render() {
    const opts = [
      { v: "all", label: "All" },
      { v: "true", label: "Yes" },
      { v: "false", label: "No" },
    ];
    this.eGui.innerHTML = `
      <div style="display:flex;flex-direction:column;gap:6px;padding:8px;min-width:120px;font-family:'JetBrains Mono',monospace;font-size:11px;background:#1c1818;color:#C9D4D4;border:1px solid #100e0e;">
        ${opts
          .map(
            (o) =>
              `<label style="display:flex;align-items:center;gap:6px;cursor:pointer;padding:2px 0;">
                <input type="radio" name="boolFilter" value="${o.v}" ${
                this.value === o.v ? "checked" : ""
              } style="accent-color:#E2BDB1;" />
                ${o.label}
              </label>`,
          )
          .join("")}
      </div>`;
    this.eGui.querySelectorAll("input").forEach((input) => {
      input.addEventListener("change", (e) => {
        this.value = (e.target as HTMLInputElement).value as BooleanFilterState["value"];
        this.filterChangedCallback({});
      });
    });
  }
}

// ── Advanced filter panel state ─────────────────────────────────

interface AdvancedFilters {
  domains: Set<string>;
  ranks: Set<string>;
  minLikes: number | null;
  maxLikes: number | null;
  dateFrom: string | null;
  dateTo: string | null;
}

const emptyFilters = (): AdvancedFilters => ({
  domains: new Set(),
  ranks: new Set(),
  minLikes: null,
  maxLikes: null,
  dateFrom: null,
  dateTo: null,
});

function filtersActive(f: AdvancedFilters): boolean {
  return (
    f.domains.size > 0 ||
    f.ranks.size > 0 ||
    f.minLikes !== null ||
    f.maxLikes !== null ||
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

const DOMAIN_OPTIONS = ["Tech", "Business", "Creative", "Lifestyle", "Education"] as const;
const RANK_OPTIONS = ["A", "B", "C"] as const;



function InfoHeader(props: CustomHeaderProps & { tooltip?: string }) {
  return (
    <div className="flex items-center gap-1.5" title={props.tooltip}>
      <span>{props.displayName}</span>
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
    filter: "agTextColumnFilter",
    filterParams: {
      filterOptions: ["contains", "notContains", "equals", "startsWith"],
      buttons: ["reset"],
    } as Record<string, unknown>,
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
    field: "caption",
    headerName: "Caption",
    flex: 2,
    minWidth: 250,
    filter: "agTextColumnFilter",
    filterParams: {
      filterOptions: ["contains", "notContains", "equals", "startsWith"],
      buttons: ["reset"],
    } as Record<string, unknown>,
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
    sortable: true,
    filter: "agNumberColumnFilter",
    filterParams: {
      filterOptions: ["equals", "notEqual", "greaterThan", "lessThan", "inRange"],
      buttons: ["reset"],
    } as Record<string, unknown>,
    cellClass: "font-data tabular-nums !justify-end",
    valueFormatter: ({ value }: { value: number }) => formatNumber(value),
  },
  {
    field: "comments_count",
    headerName: "Comments",
    width: 100,
    sortable: true,
    filter: "agNumberColumnFilter",
    filterParams: {
      filterOptions: ["equals", "notEqual", "greaterThan", "lessThan", "inRange"],
      buttons: ["reset"],
    } as Record<string, unknown>,
    cellClass: "font-data tabular-nums !justify-end",
    valueFormatter: ({ value }: { value: number }) => formatNumber(value),
  },
  {
    field: "video_view_count",
    headerName: "Views",
    width: 90,
    sortable: true,
    filter: "agNumberColumnFilter",
    filterParams: {
      filterOptions: ["equals", "notEqual", "greaterThan", "lessThan", "inRange"],
      buttons: ["reset"],
    } as Record<string, unknown>,
    cellClass: "font-data tabular-nums !justify-end",
    valueFormatter: ({ value }: { value: number }) => formatNumber(value),
  },
  {
    field: "admiralty",
    headerName: "Rank",
    width: 80,
    sortable: true,
    filter: "agTextColumnFilter",
    filterParams: {
      filterOptions: ["contains", "equals", "startsWith"],
      buttons: ["reset"],
    } as Record<string, unknown>,
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
    filter: "agTextColumnFilter",
    filterParams: {
      filterOptions: ["contains", "equals"],
      buttons: ["reset"],
    } as Record<string, unknown>,
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
    filter: "agTextColumnFilter",
    filterParams: {
      filterOptions: ["contains", "equals"],
      buttons: ["reset"],
    } as Record<string, unknown>,
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
    headerName: "Edu",
    width: 80,
    filter: BooleanFilter,
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
    headerName: "Act",
    width: 80,
    filter: BooleanFilter,
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
    sortable: true,
    filter: "agDateColumnFilter",
    filterParams: {
      filterOptions: ["equals", "greaterThan", "lessThan", "inRange"],
      buttons: ["reset"],
    } as Record<string, unknown>,
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
  const [showAdvanced, setShowAdvanced] = useState(false);
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

  // Apply external filter to AG Grid when advanced filters change
  const isExternalFilterPresent = useCallback((): boolean => {
    return filtersActive(filters);
  }, [filters]);

  const doesExternalFilterPass = useCallback(
    (node: { data: PostRow }): boolean => {
      const row = node.data;
      if (filters.domains.size > 0 && !filters.domains.has(row.gold_domain ?? ""))
        return false;
      if (filters.ranks.size > 0) {
        const rank = (row.admiralty ?? "")[0];
        if (!filters.ranks.has(rank)) return false;
      }
      if (filters.minLikes !== null && (row.likes_count ?? 0) < filters.minLikes)
        return false;
      if (filters.maxLikes !== null && (row.likes_count ?? 0) > filters.maxLikes)
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

  const clearAdvanced = () => setFilters(emptyFilters());

  const defaultColDef = useMemo(
    () => ({
      resizable: true,
      suppressMovable: false,
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

  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-lg font-bold tracking-[-0.03em] text-[#C9D4D4]">Posts</h2>
        <p className="text-[11px] text-muted font-data mt-0.5">
          {data.length.toLocaleString()} posts
          {filtersActive(filters) ? " · advanced filters active" : ""}
          {" · click column headers to filter"}
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

      {/* ── Search bar + Advanced toggle ────────────────────────── */}
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
          onClick={() => setShowAdvanced(!showAdvanced)}
          className={`px-3 py-2 text-[11px] font-data border transition-colors flex items-center gap-2
            ${showAdvanced || filtersActive(filters)
              ? "border-accent text-accent bg-[#1c1818]"
              : "border-border text-muted hover:border-accent-dim hover:text-[#C9D4D4]"
            }`}
          aria-expanded={showAdvanced}
          aria-label="Toggle advanced filters"
        >
          <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
          </svg>
          Advanced
          {filtersActive(filters) && (
            <span className="w-2 h-2 rounded-full bg-accent inline-block" />
          )}
        </button>
      </div>

      {/* ── Advanced filter panel ───────────────────────────────── */}
      {showAdvanced && (
        <div className="bg-[#1c1818] border border-border p-4 space-y-4">
          <div className="flex items-center justify-between">
            <span className="text-[11px] font-data text-muted uppercase tracking-widest">
              Advanced Filters
            </span>
            <button
              onClick={clearAdvanced}
              className="text-[10px] font-data text-muted hover:text-accent transition-colors"
            >
              Clear All
            </button>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
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
                  className="w-full bg-[#100e0e] border border-border text-[#C9D4D4] text-[12px]
                             px-2 py-1.5 placeholder:text-muted font-data
                             focus:border-accent focus:outline-none"
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
                  className="w-full bg-[#100e0e] border border-border text-[#C9D4D4] text-[12px]
                             px-2 py-1.5 placeholder:text-muted font-data
                             focus:border-accent focus:outline-none"
                  aria-label="Max likes"
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
                  className="w-full bg-[#100e0e] border border-border text-[#C9D4D4] text-[12px]
                             px-2 py-1.5 font-data focus:border-accent focus:outline-none"
                  aria-label="Date from"
                  style={{ colorScheme: "dark" }}
                />
                <input
                  type="date"
                  value={filters.dateTo ?? ""}
                  onChange={(e) =>
                    setFilters((f) => ({ ...f, dateTo: e.target.value || null }))
                  }
                  className="w-full bg-[#100e0e] border border-border text-[#C9D4D4] text-[12px]
                             px-2 py-1.5 font-data focus:border-accent focus:outline-none"
                  aria-label="Date to"
                  style={{ colorScheme: "dark" }}
                />
              </div>
            </fieldset>
          </div>
        </div>
      )}

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

      <style>{`
        .ag-theme-alpine-dark {
          --ag-background-color: #1c1818;
          --ag-header-background-color: #100e0e;
          --ag-row-hover-color: #2c2727;
          --ag-border-color: #100e0e;
          --ag-foreground-color: #C9D4D4;
          --ag-header-foreground-color: #8a8080;
          --ag-secondary-foreground-color: #6a6262;
          --ag-row-border-color: #100e0e;
          --ag-font-family: "Inter", system-ui, sans-serif;
          --ag-font-size: 13px;
          --ag-header-font-size: 10px;
          --ag-header-font-weight: 600;
          --ag-header-column-separator-display: block;
          --ag-header-column-separator-color: #100e0e;
          --ag-input-focus-border-color: #E2BDB1;
          --ag-selected-row-background-color: #1c1818;
          --ag-odd-row-background-color: #1a1616;
          --ag-range-selection-border-color: #E2BDB1;
        }
        .ag-header-cell-text {
          text-transform: uppercase;
          letter-spacing: 0.1em;
          font-family: "JetBrains Mono", monospace !important;
        }
        .ag-cell.\\!block {
          display: block !important;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .ag-cell.flex {
          display: flex;
          align-items: center;
        }
        .ag-cell:not(.\\!block):not(.flex) {
          display: flex;
          align-items: center;
        }

        /* Filter popup dark theme */
        .ag-menu,
        .ag-filter-wrapper,
        .ag-filter,
        .ag-simple-filter-body-wrapper,
        .ag-tabs,
        .ag-tab {
          background-color: #1c1818 !important;
          color: #C9D4D4 !important;
          font-family: "JetBrains Mono", monospace !important;
          font-size: 11px !important;
          border-color: #100e0e !important;
        }
        .ag-filter input,
        .ag-filter select,
        .ag-simple-filter input,
        .ag-number-field input {
          background-color: #100e0e !important;
          color: #C9D4D4 !important;
          border: 1px solid #100e0e !important;
          font-family: "JetBrains Mono", monospace !important;
          font-size: 11px !important;
          padding: 4px 8px !important;
        }
        .ag-filter input:focus,
        .ag-simple-filter input:focus,
        .ag-number-field input:focus {
          border-color: #E2BDB1 !important;
          outline: none !important;
        }
        .ag-filter .ag-filter-apply-panel button,
        .ag-tab {
          color: #C9D4D4 !important;
        }
        .ag-tab.ag-tab-selected {
          color: #E2BDB1 !important;
          border-bottom-color: #E2BDB1 !important;
        }
        .ag-icon {
          color: #8a8080 !important;
        }
        .ag-icon:hover {
          color: #E2BDB1 !important;
        }
        .ag-filter .ag-filter-header-container {
          border-bottom-color: #100e0e !important;
        }
        /* Boolean filter radio styling inherited from component inline styles */
      `}</style>
    </div>
  );
}
