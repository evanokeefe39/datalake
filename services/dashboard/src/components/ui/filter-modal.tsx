import { useEffect } from "react";
import { X } from "lucide-react";

interface FilterModalProps {
  open: boolean;
  onClose: () => void;
  title?: string;
  children: React.ReactNode;
}

/**
 * Scrollable modal shell for filter criteria. The header stays pinned while
 * the body scrolls, so a large set of filter controls never pushes the close
 * button off-screen. Closes on Escape and on backdrop click.
 */
export function FilterModal({ open, onClose, title = "Filters", children }: FilterModalProps) {
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center p-4 sm:p-10 overflow-y-auto">
      <div className="fixed inset-0 bg-black/60" onClick={onClose} aria-hidden />
      <div className="relative w-full max-w-2xl bg-surface border border-border max-h-[80vh] overflow-y-auto">
        <div className="sticky top-0 flex items-center justify-between border-b border-border bg-surface px-5 py-4 z-10">
          <span className="text-[11px] font-data uppercase tracking-widest text-muted">
            {title}
          </span>
          <button
            type="button"
            onClick={onClose}
            className="text-muted hover:text-accent transition-colors"
            aria-label="Close filters"
          >
            <X className="w-4 h-4" />
          </button>
        </div>
        <div className="p-5">{children}</div>
      </div>
    </div>
  );
}
