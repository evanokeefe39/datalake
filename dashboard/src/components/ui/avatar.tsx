import { useState, useEffect } from "react";
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
  const [src, setSrc] = useState<string | null>(null);
  const [fallback, setFallback] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const color = hashColor(username);
    const url = `https://api.dicebear.com/9.x/identicon/svg?seed=${encodeURIComponent(username)}&backgroundColor=000000&foregroundColor=${color}&size=${size * 2}`;

    // Try cached Instagram pic first
    fetch(`/api/media/avatar/${encodeURIComponent(username)}`)
      .then((r) => r.json())
      .then((data) => {
        if (!cancelled && data.url && !data.url.includes("dicebear")) {
          setSrc(data.url);
        } else {
          setSrc(url);
        }
      })
      .catch(() => {
        if (!cancelled) setSrc(url);
      });
    return () => { cancelled = true; };
  }, [username, size]);

  if (!src) {
    return (
      <div
        className={cn("border border-border bg-surface flex-shrink-0", className)}
        style={{ width: size, height: size }}
      />
    );
  }

  return (
    <img
      src={src}
      alt={username}
      className={cn("flex-shrink-0", className)}
      style={{ width: size, height: size }}
      onError={() => {
        if (!fallback) {
          setFallback(true);
          const color = hashColor(username);
          setSrc(
            `https://api.dicebear.com/9.x/identicon/svg?seed=${encodeURIComponent(username)}&backgroundColor=000000&foregroundColor=${color}&size=${size * 2}`
          );
        }
      }}
    />
  );
}
