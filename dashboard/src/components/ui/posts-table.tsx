import { useCallback, useMemo, type Ref } from "react";
import { Link } from "@tanstack/react-router";
import { AgGridReact } from "ag-grid-react";
import type { CustomHeaderProps } from "ag-grid-react";
import type {
  ColDef,
  GridReadyEvent,
  GridSizeChangedEvent,
  IRowNode,
  RowClickedEvent,
} from "ag-grid-community";
import { AllCommunityModule, ModuleRegistry } from "ag-grid-community";
import { Avatar } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { Thumbnail } from "@/components/ui/thumbnail";
import { PlatformIcon } from "@/components/ui/platform-icon";
import { Info } from "lucide-react";
import type { PostRow } from "@/lib/api";

ModuleRegistry.registerModules([AllCommunityModule]);

// ── Helpers ─────────────────────────────────────────────────────

export function admiraltyVariant(
  tier: string | null,
): "green" | "yellow" | "orange" | "default" {
  if (!tier) return "default";
  const t = tier.charAt(0);
  if (t === "A") return "green";
  if (t === "B") return "yellow";
  if (t === "C") return "orange";
  return "default";
}

export function formatNumber(n: number | null | undefined): string {
  if (n == null || n === 0) return "--";
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

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

/** Open the post's Instagram page in a new tab. */
export function openPostInNewTab(row: PostRow): void {
  if (row.shortcode) {
    window.open(
      `https://www.instagram.com/p/${row.shortcode}/`,
      "_blank",
      "noopener,noreferrer",
    );
  }
}

export const POST_ROW_CLASS = "cursor-pointer";

// ── Canonical post columns (shared by /posts and /creators/$id) ──

export const POST_COLUMN_DEFS: ColDef<PostRow>[] = [
  {
    field: "shortcode",
    headerName: "",
    width: 64,
    pinned: "left",
    sortable: false,
    cellClass: "!justify-center",
    cellRenderer: ({ data }: { data: PostRow }) =>
      data.shortcode ? <Thumbnail shortcode={data.shortcode} size={48} /> : null,
  },
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
          onClick={(e) => e.stopPropagation()}
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
    field: "relative_performance",
    headerName: "Perf",
    width: 100,
    cellClass: "flex !items-center !justify-center",
    cellRenderer: ({ value }: { value: string | null }) => {
      if (value === "hot") return <Badge variant="red">Hot</Badge>;
      if (value === "standout") return <Badge variant="yellow">Standout</Badge>;
      return <span className="text-muted">—</span>;
    },
  },
  {
    field: "baseline_likes",
    headerName: "Baseline",
    width: 92,
    headerComponent: InfoHeader,
    cellClass: "font-data tabular-nums !justify-end",
    headerComponentParams: {
      tooltip:
        "Creator's typical likes when this post was published (trailing point-in-time baseline).",
    },
    valueFormatter: ({ value }: { value: number | null }) =>
      value != null ? formatNumber(value) : "—",
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

// ── Shared grid ─────────────────────────────────────────────────

interface PostsTableProps {
  rows: PostRow[];
  /** Extra columns appended after the canonical set (e.g. creator-page relative performance). */
  extraColumns?: ColDef<PostRow>[];
  /** Hide the Profile (owner_username) column — for a single-creator context. */
  hideProfileColumn?: boolean;
  quickFilterText?: string;
  pagination?: boolean;
  gridRef?: Ref<AgGridReact<PostRow>>;
  isExternalFilterPresent?: () => boolean;
  doesExternalFilterPass?: (node: IRowNode<PostRow>) => boolean;
  onFilterChanged?: () => void;
}

/**
 * The canonical posts table. Row click opens the post in a new tab;
 * the Profile link stops propagation so it doesn't also open the tab.
 */
export function PostsTable({
  rows,
  extraColumns,
  hideProfileColumn = false,
  quickFilterText,
  pagination = false,
  gridRef,
  isExternalFilterPresent,
  doesExternalFilterPass,
  onFilterChanged,
}: PostsTableProps) {
  const columnDefs = useMemo(() => {
    const base = hideProfileColumn
      ? POST_COLUMN_DEFS.filter((c) => c.field !== "owner_username")
      : POST_COLUMN_DEFS;
    return extraColumns?.length ? [...base, ...extraColumns] : base;
  }, [extraColumns, hideProfileColumn]);

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

  const onRowClicked = useCallback((event: RowClickedEvent<PostRow>) => {
    if (event.data) openPostInNewTab(event.data);
  }, []);

  return (
    <div className="ag-theme-alpine-dark h-[calc(100vh-200px)] w-full border border-border">
      <AgGridReact<PostRow>
        ref={gridRef}
        onGridSizeChanged={onGridSizeChanged}
        rowData={rows}
        columnDefs={columnDefs}
        defaultColDef={defaultColDef}
        onGridReady={onGridReady}
        onRowClicked={onRowClicked}
        rowClass={POST_ROW_CLASS}
        pagination={pagination}
        paginationPageSize={50}
        paginationPageSizeSelector={[25, 50, 100, 200]}
        quickFilterText={quickFilterText}
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
  );
}
