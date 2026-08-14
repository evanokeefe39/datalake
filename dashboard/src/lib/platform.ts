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
  if (platform === "instagram") return "cyan";
  if (platform === "tiktok") return "magenta";
  if (platform === "youtube") return "red";
  return "default";
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
