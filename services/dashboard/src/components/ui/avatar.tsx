import { useState } from "react";
import { cn } from "@/lib/utils";

interface AvatarProps {
  username: string;
  size?: number;
  className?: string;
}

const COLORS = ["00ffff", "ff00ff", "00ff41", "ff6b35", "ffd700"];

function hashColor(username: string): string {
  let hash = 0;
  for (let i = 0; i < username.length; i++) {
    hash = username.charCodeAt(i) + ((hash << 5) - hash);
  }
  return COLORS[Math.abs(hash) % COLORS.length];
}

export function Avatar({ username, size = 40, className }: AvatarProps) {
  const [fallback, setFallback] = useState(false);
  const color = hashColor(username);
  const dicebear = `https://api.dicebear.com/9.x/identicon/svg?seed=${encodeURIComponent(username)}&backgroundColor=000000&foregroundColor=${color}&size=${size * 2}`;
  const src = fallback
    ? dicebear
    : `/api/media/avatar/${encodeURIComponent(username)}`;

  return (
    <img
      src={src}
      alt={username}
      loading="lazy"
      decoding="async"
      className={cn("flex-shrink-0", className)}
      style={{ width: size, height: size }}
      onError={() => setFallback(true)}
    />
  );
}
