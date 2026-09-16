export const PLATFORMS = ["instagram", "tiktok", "youtube"] as const;

export type BadgeVariant =
  | "default"
  | "green"
  | "orange"
  | "yellow"
  | "red"
  | "cyan"
  | "magenta"
  | "accent";

export function platformBadge(platform: string): BadgeVariant {
  if (!platform) return "default";
  const p = platform.toLowerCase();
  if (p === "instagram") return "cyan";
  if (p === "tiktok") return "magenta";
  if (p === "youtube") return "red";
  if (p === "reddit") return "orange";
  if (p === "twitter" || p === "x") return "accent";
  return "default";
}

/** Normalize a platform key to a display label ("twitter" → "X"). */
export function platformLabel(platform: string): string {
  if (!platform) return "";
  const p = platform.toLowerCase();
  if (p === "twitter" || p === "x") return "X";
  return p.charAt(0).toUpperCase() + p.slice(1);
}

/**
 * Normalize a profile handle the same way the server does: strip a leading
 * `@`, and reduce a full URL to its final path segment (query string dropped).
 */
export function normalizeHandle(value: string): string {
  let h = value.trim().replace(/^@/, "");
  if (h.startsWith("http")) {
    h = h.replace(/\/+$/, "").split("/").pop() ?? h;
    h = h.split("?")[0];
  }
  return h;
}
