/** Map a numeric admiralty score (0-3) to its letter tier badge. */
export function admiraltyTier(score: number | undefined | null): string {
  if (score == null) return "--";
  if (score >= 4) return "A+";
  if (score >= 3) return "A";
  if (score >= 2) return "B";
  if (score >= 1) return "C";
  return "D";
}
