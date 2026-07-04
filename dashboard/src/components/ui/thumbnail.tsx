import { useState, useEffect } from "react";
import { ImageOff } from "lucide-react";
import { cn } from "@/lib/utils";

interface ThumbnailProps {
  shortcode: string;
  size?: number;
  className?: string;
}

export function Thumbnail({ shortcode, size = 120, className }: ThumbnailProps) {
  const [url, setUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    if (!shortcode) {
      setLoading(false);
      setError(true);
      return;
    }
    let cancelled = false;
    setLoading(true);
    fetch(`/api/media/thumbnail/${encodeURIComponent(shortcode)}`)
      .then((r) => r.json())
      .then((data) => {
        if (!cancelled) {
          if (data.url) setUrl(data.url);
          else setError(true);
          setLoading(false);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setError(true);
          setLoading(false);
        }
      });
    return () => { cancelled = true; };
  }, [shortcode]);

  if (loading) {
    return (
      <div
        className={cn("border border-border bg-[#100e0e] flex-shrink-0 animate-pulse", className)}
        style={{ width: size, height: size * 0.75 }}
      />
    );
  }

  if (error || !url) {
    return (
      <a
        href={`https://www.instagram.com/p/${shortcode}/`}
        target="_blank"
        rel="noopener noreferrer"
        className={cn(
          "border border-border bg-[#100e0e] flex items-center justify-center flex-shrink-0",
          "hover:border-accent-dim transition-colors",
          className,
        )}
        style={{ width: size, height: size * 0.75 }}
      >
        <ImageOff className="text-muted w-4 h-4" />
      </a>
    );
  }

  return (
    <img
      src={url}
      alt=""
      className={cn("object-cover flex-shrink-0", className)}
      style={{ width: size, height: size * 0.75 }}
      onError={() => setError(true)}
    />
  );
}
