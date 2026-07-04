import { cn } from "@/lib/utils";

interface MetricCardProps {
  label: string;
  value: string | number;
  sub?: string;
  accent?: "accent" | "green" | "orange" | "yellow" | "cyan" | "magenta";
  className?: string;
  span?: "full" | "wide" | "normal";
}

const accentColors: Record<string, string> = {
  accent: "text-accent",
  green: "text-accent-green",
  orange: "text-accent-orange",
  yellow: "text-accent-yellow",
  cyan: "text-accent-cyan",
  magenta: "text-accent-magenta",
};

const spanClasses: Record<string, string> = {
  full: "col-span-full",
  wide: "col-span-2",
  normal: "",
};

export function MetricCard({
  label,
  value,
  sub,
  accent = "accent",
  className,
  span = "normal",
}: MetricCardProps) {
  const color = accentColors[accent];

  return (
    <div
      className={cn(
        "bg-surface border border-border p-5",
        "hover:border-accent-dim transition-colors duration-200",
        "group",
        spanClasses[span],
        className,
      )}
    >
      <div className="text-[10px] font-semibold text-muted uppercase tracking-[0.15em] mb-3 font-data">
        {label}
      </div>
      <div className={cn("text-3xl font-bold tracking-tight", color)}>
        {value}
      </div>
      {sub && (
        <div className="mt-2 text-[11px] text-muted font-data">{sub}</div>
      )}
    </div>
  );
}
