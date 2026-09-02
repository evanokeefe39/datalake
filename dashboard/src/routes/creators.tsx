import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "@tanstack/react-router";
import { AgGridReact } from "ag-grid-react";
import type { ColDef, GridReadyEvent, GridSizeChangedEvent } from "ag-grid-community";
import { AllCommunityModule, ModuleRegistry } from "ag-grid-community";
import { Avatar } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { FilterModal } from "@/components/ui/filter-modal";
import { PlatformIcon } from "@/components/ui/platform-icon";
import { CreatorModal } from "@/components/ui/creator-modal";
import { Users, Search } from "lucide-react";
import { fetchCreators, type Creator } from "@/lib/api";
import { admiraltyTier } from "@/lib/admiralty";

ModuleRegistry.registerModules([AllCommunityModule]);

const COLUMN_DEFS: ColDef<Creator>[] = [
  {
    field: "name",
    headerName: "Creator",
    pinned: "left",
    flex: 2,
    minWidth: 220,
    cellClass: "flex !items-center",
    cellRenderer: ({ data }: { data: Creator }) => (
      <Link
        to="/creators/$id"
        params={{ id: String(data.id) }}
        className="flex items-center gap-2 py-1 hover:opacity-75 transition-opacity"
      >
        <Avatar username={data.avatar_handle ?? data.name} size={28} />
        <span className="text-cyan-400 font-semibold text-[13px]">{data.name}</span>
      </Link>
    ),
  },
  {
    field: "platforms",
    headerName: "Platforms",
    width: 180,
    valueGetter: (params) => params.data?.platforms.join(", ") ?? "",
    cellClass: "flex !items-center",
    cellRenderer: ({ data }: { data: Creator }) =>
      data.platforms.length === 0 ? (
        <span className="text-muted text-xs">—</span>
      ) : (
        <span className="flex gap-2 flex-wrap">
          {data.platforms.map((p) => (
            <PlatformIcon key={p} platform={p} size={16} />
          ))}
        </span>
      ),
  },
  {
    field: "total_posts",
    headerName: "Posts",
    width: 110,
    cellClass: "font-data tabular-nums !justify-center",
  },
  {
    field: "enriched_posts",
    headerName: "Enriched",
    width: 110,
    cellClass: "font-data tabular-nums !justify-center",
    cellRenderer: ({ value }: { value: number }) =>
      value > 0 ? (
        <span className="text-accent-green">{value}</span>
      ) : (
        <span className="text-muted">0</span>
      ),
  },
  {
    field: "educational_rate",
    headerName: "EDU",
    width: 90,
    cellClass: "font-data tabular-nums !justify-center",
    cellRenderer: ({ value }: { value: number }) =>
      `${((value ?? 0) * 100).toFixed(0)}%`,
  },
  {
    field: "actionable_rate",
    headerName: "ACT",
    width: 90,
    cellClass: "font-data tabular-nums !justify-center",
    cellRenderer: ({ value }: { value: number }) =>
      `${((value ?? 0) * 100).toFixed(0)}%`,
  },
  {
    field: "admiralty_score",
    headerName: "ADMIRALTY",
    width: 130,
    cellClass: "!justify-center",
    cellRenderer: ({ value }: { value: number }) => {
      const tier = admiraltyTier(value);
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
    field: "avg_likes",
    headerName: "Avg Likes",
    width: 120,
    cellClass: "font-data tabular-nums !justify-center",
    cellRenderer: ({ value }: { value: number }) =>
      (value ?? 0).toLocaleString(),
  },
  {
    field: "max_likes",
    headerName: "Max Likes",
    width: 120,
    cellClass: "font-data tabular-nums !justify-center",
    cellRenderer: ({ value }: { value: number }) =>
      (value ?? 0).toLocaleString(),
  },
  {
    field: "standout_count",
    headerName: "Standouts",
    width: 120,
    cellClass: "font-data tabular-nums !justify-center",
    cellRenderer: ({ value }: { value: number }) =>
      value > 0 ? (
        <span className="text-accent-yellow">{value}</span>
      ) : (
        <span className="text-muted">0</span>
      ),
  },
  {
    field: "hot_count",
    headerName: "Hot",
    width: 100,
    cellClass: "font-data tabular-nums !justify-center",
    cellRenderer: ({ value }: { value: number }) =>
      value > 0 ? (
        <span className="text-accent-red">{value}</span>
      ) : (
        <span className="text-muted">0</span>
      ),
  },
  {
    field: "momentum_ratio",
    headerName: "Momentum",
    width: 130,
    cellClass: "!justify-center flex !items-center gap-1",
    cellRenderer: ({ data }: { data: Creator }) =>
      data.is_rising ? (
        <Badge variant="green">Rising</Badge>
      ) : data.momentum_ratio != null ? (
        <span className="font-data tabular-nums text-xs">
          {data.momentum_ratio >= 1 ? "+" : ""}
          {Math.round((data.momentum_ratio - 1) * 100)}%
        </span>
      ) : (
        <span className="text-muted text-xs">—</span>
      ),
  },
  {
    field: "dominant_domain",
    headerName: "Dominant Domain",
    width: 170,
    cellClass: "!justify-center",
    cellRenderer: ({ data }: { data: Creator }) =>
      data.dominant_domain ? (
        <span className="text-xs">
          {data.dominant_domain}{" "}
          <span className="text-muted font-data">
            ({data.dominant_domain_posts})
          </span>
        </span>
      ) : (
        <span className="text-muted text-xs">—</span>
      ),
  },
];

export default function CreatorsPage() {
  const [creators, setCreators] = useState<Creator[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [filterOpen, setFilterOpen] = useState(false);
  const [filterText, setFilterText] = useState("");
  const gridRef = useRef<AgGridReact<Creator>>(null);

  const refresh = () => {
    setLoading(true);
    fetchCreators()
      .then(setCreators)
      .catch((e: unknown) => setError(String(e)))
      .finally(() => setLoading(false));
  };

  useEffect(refresh, []);

  const defaultColDef = useMemo(
    () => ({ resizable: true, sortable: true, suppressHeaderMenuButton: true }),
    [],
  );

  const onGridReady = useCallback((params: GridReadyEvent) => {
    params.api.sizeColumnsToFit();
  }, []);

  const onGridSizeChanged = useCallback((params: GridSizeChangedEvent) => {
    params.api.sizeColumnsToFit();
  }, []);

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-4">
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
        <div className="flex items-center gap-3">
          <button
            onClick={() => setFilterOpen(true)}
            className={`px-3 py-2 text-[11px] font-data border transition-colors flex items-center gap-2
              ${filterText
                ? "border-accent text-accent bg-[#1c1818]"
                : "border-border text-muted hover:border-accent-dim hover:text-[#C9D4D4]"
              }`}
            aria-label="Open filters"
          >
            <Search className="w-3.5 h-3.5" />
            Filter
            {filterText && <span className="w-2 h-2 rounded-full bg-accent inline-block" />}
          </button>
          <button
            onClick={() => setModalOpen(true)}
            className="border border-accent text-accent px-4 py-2 text-sm hover:bg-accent hover:text-black transition-colors"
          >
            Add creator
          </button>
        </div>
      </div>

      {error && <p className="text-red-400 text-xs">{error}</p>}

      <div className="ag-theme-alpine-dark h-[calc(100vh-200px)] w-full border border-border">
        <AgGridReact<Creator>
          ref={gridRef}
          rowData={creators}
          columnDefs={COLUMN_DEFS}
          defaultColDef={defaultColDef}
          onGridReady={onGridReady}
          onGridSizeChanged={onGridSizeChanged}
          loading={loading}
          suppressCellFocus
          quickFilterText={filterText}
          rowHeight={40}
          headerHeight={36}
        />
      </div>

      <FilterModal
        open={filterOpen}
        onClose={() => setFilterOpen(false)}
        title="Filters"
      >
        <div className="space-y-2">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted" />
            <input
              type="text"
              placeholder="Search creators, handles, platforms..."
              value={filterText}
              onChange={(e) => setFilterText(e.target.value)}
              className="w-full bg-[#100e0e] border border-border text-[#C9D4D4] text-[13px]
                         pl-10 pr-4 py-2 placeholder:text-muted
                         focus:border-accent focus:outline-none transition-colors font-data"
              aria-label="Search creators"
            />
          </div>
          {filterText.length > 0 && (
            <button
              onClick={() => setFilterText("")}
              className="text-[10px] font-data text-muted hover:text-accent transition-colors"
            >
              Clear
            </button>
          )}
        </div>
      </FilterModal>

      <CreatorModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        onSaved={refresh}
      />
    </div>
  );
}
