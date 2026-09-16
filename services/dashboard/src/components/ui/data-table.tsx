import { useMemo, useState } from "react";
import { ChevronDown, ChevronUp, ChevronsUpDown } from "lucide-react";
import { cn } from "@/lib/utils";

export interface DataTableColumn<T> {
  key: string;
  header: React.ReactNode;
  className?: string;
  /** Columns are sortable by default; set false for purely visual columns. */
  sortable?: boolean;
  /** Custom sort key for cells that render non-scalar React nodes. */
  sortValue?: (row: T) => string | number | null | undefined;
  render?: (row: T) => React.ReactNode;
}

interface DataTableProps<T> {
  columns: DataTableColumn<T>[];
  data: T[];
  rowKey: (row: T) => string;
  className?: string;
  emptyMessage?: string;
  loading?: boolean;
  initialSort?: { key: string; dir: "asc" | "desc" };
}

type SortState = { key: string; dir: "asc" | "desc" } | null;

/** Null/empty values sort last; numbers compare numerically, else as text. */
function compareValues(a: unknown, b: unknown): number {
  const aEmpty = a == null || a === "";
  const bEmpty = b == null || b === "";
  if (aEmpty && bEmpty) return 0;
  if (aEmpty) return 1;
  if (bEmpty) return -1;
  if (typeof a === "number" && typeof b === "number") return a - b;
  return String(a).localeCompare(String(b), undefined, {
    numeric: true,
    sensitivity: "base",
  });
}

export function DataTable<T extends object>({
  columns,
  data,
  rowKey,
  className,
  emptyMessage = "No data.",
  loading = false,
  initialSort,
}: DataTableProps<T>) {
  const [sort, setSort] = useState<SortState>(initialSort ?? null);

  const sorted = useMemo(() => {
    if (!sort) return data;
    const col = columns.find((c) => c.key === sort.key);
    if (!col) return data;
    const getValue =
      col.sortValue ?? ((row: T) => (row as Record<string, unknown>)[col.key]);
    const dir = sort.dir === "asc" ? 1 : -1;
    return [...data].sort((a, b) => dir * compareValues(getValue(a), getValue(b)));
  }, [data, columns, sort]);

  const toggleSort = (col: DataTableColumn<T>) => {
    if (col.sortable === false) return;
    setSort((prev) =>
      prev?.key === col.key
        ? { key: col.key, dir: prev.dir === "asc" ? "desc" : "asc" }
        : { key: col.key, dir: "asc" },
    );
  };

  if (loading) {
    return (
      <div className={cn("bg-surface border border-border", className)}>
        <div className="p-10 text-center text-muted font-data text-xs animate-pulse tracking-widest">
          LOADING
        </div>
      </div>
    );
  }

  return (
    <div className={cn("bg-surface border border-border overflow-x-auto", className)}>
      <table className="w-full">
        <thead>
          <tr className="border-b border-border bg-[#100e0e]">
            {columns.map((col) => {
              const sortable = col.sortable !== false;
              const active = sort?.key === col.key;
              return (
                <th
                  key={col.key}
                  className={cn(
                    "px-4 py-3 text-left text-[10px] font-semibold uppercase tracking-[0.12em] text-muted font-data",
                    col.className,
                  )}
                  aria-sort={
                    active ? (sort!.dir === "asc" ? "ascending" : "descending") : undefined
                  }
                >
                  {sortable ? (
                    <button
                      type="button"
                      onClick={() => toggleSort(col)}
                      className={cn(
                        "inline-flex items-center gap-1 uppercase tracking-[0.12em] font-data",
                        "hover:text-[#C9D4D4] transition-colors",
                        active && "text-accent",
                      )}
                      aria-label={`Sort by ${typeof col.header === "string" ? col.header : col.key}`}
                    >
                      <span>{col.header}</span>
                      {active ? (
                        sort!.dir === "asc" ? (
                          <ChevronUp className="w-3 h-3" aria-hidden />
                        ) : (
                          <ChevronDown className="w-3 h-3" aria-hidden />
                        )
                      ) : (
                        <ChevronsUpDown
                          className="w-3 h-3 opacity-40"
                          aria-hidden
                        />
                      )}
                    </button>
                  ) : (
                    col.header
                  )}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {data.length === 0 ? (
            <tr>
              <td
                colSpan={columns.length}
                className="px-4 py-14 text-center text-muted text-xs font-data"
              >
                {emptyMessage}
              </td>
            </tr>
          ) : (
            sorted.map((row) => (
              <tr
                key={rowKey(row)}
                className={cn(
                  "border-b border-[#100e0e] hover:bg-[#1c1818] transition-colors",
                )}
              >
                {columns.map((col) => (
                  <td
                    key={col.key}
                    className={cn("px-4 py-3 text-[13px]", col.className)}
                  >
                    {col.render
                      ? col.render(row)
                      : String((row as Record<string, unknown>)[col.key] ?? "")}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
