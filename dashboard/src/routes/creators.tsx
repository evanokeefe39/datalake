import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "@tanstack/react-router";
import { AgGridReact } from "ag-grid-react";
import type { ColDef, GridReadyEvent, GridSizeChangedEvent } from "ag-grid-community";
import { AllCommunityModule, ModuleRegistry } from "ag-grid-community";
import { Badge } from "@/components/ui/badge";
import { CreatorModal } from "@/components/ui/creator-modal";
import { Users } from "lucide-react";
import { platformBadge } from "@/lib/platform";
import { fetchCreators, type Creator } from "@/lib/api";

ModuleRegistry.registerModules([AllCommunityModule]);

const COLUMN_DEFS: ColDef<Creator>[] = [
  {
    field: "name",
    headerName: "Name",
    pinned: "left",
    flex: 2,
    minWidth: 200,
    filter: "agTextColumnFilter",
    filterParams: {
      filterOptions: ["contains", "notContains", "equals", "startsWith"],
      buttons: ["reset"],
    } as Record<string, unknown>,
    cellClass: "flex !items-center",
    cellRenderer: ({ data }: { data: Creator }) => (
      <Link
        to="/creators/$id"
        params={{ id: String(data.id) }}
        className="text-cyan-400 font-semibold hover:opacity-75 transition-opacity"
      >
        {data.name}
      </Link>
    ),
  },
  {
    field: "profile_count",
    headerName: "Profiles",
    width: 110,
    sortable: true,
    filter: "agNumberColumnFilter",
    filterParams: {
      filterOptions: ["equals", "notEqual", "greaterThan", "lessThan", "inRange"],
      buttons: ["reset"],
    } as Record<string, unknown>,
    cellClass: "font-data tabular-nums !justify-center",
  },
  {
    field: "platforms",
    headerName: "Platforms",
    width: 220,
    filter: "agTextColumnFilter",
    filterParams: {
      filterOptions: ["contains", "equals"],
      buttons: ["reset"],
    } as Record<string, unknown>,
    valueGetter: (params) => params.data?.platforms.join(", ") ?? "",
    cellClass: "flex !items-center",
    cellRenderer: ({ data }: { data: Creator }) =>
      data.platforms.length === 0 ? (
        <span className="text-muted">—</span>
      ) : (
        <span className="flex gap-1.5 flex-wrap">
          {data.platforms.map((p) => (
            <Badge key={p} variant={platformBadge(p)}>
              {p}
            </Badge>
          ))}
        </span>
      ),
  },
  {
    field: "total_posts",
    headerName: "Posts",
    width: 110,
    sortable: true,
    filter: "agNumberColumnFilter",
    filterParams: {
      filterOptions: ["equals", "notEqual", "greaterThan", "lessThan", "inRange"],
      buttons: ["reset"],
    } as Record<string, unknown>,
    cellClass: "font-data tabular-nums !justify-center",
  },
];

export default function CreatorsPage() {
  const [creators, setCreators] = useState<Creator[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [modalOpen, setModalOpen] = useState(false);
  const gridRef = useRef<AgGridReact<Creator>>(null);

  const refresh = () => {
    setLoading(true);
    fetchCreators()
      .then(setCreators)
      .catch((e: unknown) => setError(String(e)))
      .finally(() => setLoading(false));
  };

  useEffect(refresh, []);

  const defaultColDef = useMemo(() => ({ resizable: true }), []);

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
        <button
          onClick={() => setModalOpen(true)}
          className="border border-accent text-accent px-4 py-2 text-sm hover:bg-accent hover:text-black transition-colors"
        >
          Add creator
        </button>
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
          rowHeight={40}
          headerHeight={36}
        />
      </div>

      <CreatorModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        onSaved={refresh}
      />
    </div>
  );
}
