import { useState, useEffect } from "react";
import { ImageOff } from "lucide-react";
import { cn } from "@/lib/utils";

interface ThumbnailProps {
  shortcode: string;
  size?: number;
  className?: string;
}

export function Thumbnail({ shortcode, size = 120, className }: ThumbnailProps) {
  const [error, setError] = useState(false);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    setError(false);
    setLoaded(false);
  }, [shortcode]);

  if (!shortcode || error) {
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
    <div
      className={cn("relative flex-shrink-0", className)}
      style={{ width: size, height: size * 0.75 }}
    >
      {!loaded && (
        <div className="absolute inset-0 border border-border bg-[#100e0e] animate-pulse" />
      )}
      <img
        src={`/api/media/thumbnail/${encodeURIComponent(shortcode)}`}
        alt=""
        className="absolute inset-0 w-full h-full object-cover"
        onLoad={() => setLoaded(true)}
        onError={() => setError(true)}
      />
    </div>
  );
}
