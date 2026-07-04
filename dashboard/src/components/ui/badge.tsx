import { cn } from "@/lib/utils";

interface BadgeProps {
  children: React.ReactNode;
  variant?: "default" | "green" | "orange" | "yellow" | "red" | "cyan" | "magenta" | "accent";
  className?: string;
}

const variants: Record<string, string> = {
  default: "border-border text-muted",
  accent: "border-accent text-accent",
  green: "border-accent-green text-accent-green",
  orange: "border-accent-orange text-accent-orange",
  yellow: "border-accent-yellow text-accent-yellow",
  red: "border-accent-red text-accent-red",
  cyan: "border-accent-cyan text-accent-cyan",
  magenta: "border-accent-magenta text-accent-magenta",
};

export function Badge({ children, variant = "default", className }: BadgeProps) {
  return (
    <span
      className={cn(
        "inline-flex items-center border px-2.5 py-0.5 text-[10px] font-semibold uppercase tracking-[0.1em]",
        "font-data",
        variants[variant],
        className,
      )}
    >
      {children}
    </span>
  );
}
