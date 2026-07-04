import { cn } from "@/lib/utils";

interface DataTableProps<T> {
  columns: {
    key: string;
    header: string;
    className?: string;
    render?: (row: T) => React.ReactNode;
  }[];
  data: T[];
  rowKey: (row: T) => string;
  className?: string;
  emptyMessage?: string;
  loading?: boolean;
}

export function DataTable<T extends Record<string, unknown>>({
  columns,
  data,
  rowKey,
  className,
  emptyMessage = "No data.",
  loading = false,
}: DataTableProps<T>) {
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
            {columns.map((col) => (
              <th
                key={col.key}
                className={cn(
                  "px-4 py-3 text-left text-[10px] font-semibold uppercase tracking-[0.12em] text-muted font-data",
                  col.className,
                )}
              >
                {col.header}
              </th>
            ))}
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
            data.map((row, i) => (
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
                    {col.render ? col.render(row) : String(row[col.key] ?? "")}
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
