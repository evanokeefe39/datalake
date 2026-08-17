import { useEffect, useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
import type { ColDef } from "ag-grid-community";
import { Card } from "@/components/ui/card";
import { Avatar } from "@/components/ui/avatar";
import { PlatformIcon } from "@/components/ui/platform-icon";
import { PostsTable } from "@/components/ui/posts-table";
import { CreatorModal } from "@/components/ui/creator-modal";
import { Badge } from "@/components/ui/badge";
import { Users, ArrowLeft } from "lucide-react";
import {
  fetchCreator,
  fetchCreatorPosts,
  type CreatorDetail,
  type PostRow,
} from "@/lib/api";

export default function CreatorPage() {
  const { id } = useParams({ from: "/creators/$id" });
  const creatorId = Number(id);

  const [creator, setCreator] = useState<CreatorDetail | null>(null);
  const [posts, setPosts] = useState<PostRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [postsLoading, setPostsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [modalOpen, setModalOpen] = useState(false);

  const loadCreator = () => {
    setLoading(true);
    setError(null);
    setCreator(null);
    fetchCreator(creatorId)
      .then(setCreator)
      .catch((e: unknown) => setError(String(e)))
      .finally(() => setLoading(false));
  };

  const loadPosts = () => {
    setPostsLoading(true);
    fetchCreatorPosts(creatorId)
      .then(setPosts)
      .catch((e: unknown) => setError(String(e)))
      .finally(() => setPostsLoading(false));
  };

  useEffect(loadCreator, [creatorId]);
  useEffect(loadPosts, [creatorId]);

  const refreshAll = () => {
    loadCreator();
    loadPosts();
  };

  if (loading) {
    return (
      <div className="text-sm text-muted font-data tracking-widest animate-pulse">
        LOADING
      </div>
    );
  }

  if (!creator) {
    return (
      <div className="space-y-4">
        <Link
          to="/creators"
          className="inline-flex items-center gap-2 text-[11px] text-muted hover:text-white transition-colors"
        >
          <ArrowLeft className="w-3.5 h-3.5" /> Back to creators
        </Link>
        <Card className="p-8 text-sm text-muted text-center">
          Creator not found.
        </Card>
      </div>
    );
  }

  const avatarHandle = creator.profiles[0]?.handle;
  const platforms = [...new Set(creator.profiles.map((p) => p.platform))];

  return (
    <div className="space-y-6">
      <Link
        to="/creators"
        className="inline-flex items-center gap-2 text-[11px] text-muted hover:text-white transition-colors"
      >
        <ArrowLeft className="w-3.5 h-3.5" /> Back to creators
      </Link>

      <div className="flex items-center gap-4">
        {avatarHandle ? (
          <Avatar username={avatarHandle} size={48} />
        ) : (
          <div className="w-12 h-12 border border-border flex items-center justify-center">
            <Users className="w-5 h-5 text-muted" />
          </div>
        )}
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <h2 className="text-xl font-bold tracking-[-0.03em] text-white">
              {creator.name}
            </h2>
            {platforms.map((p) => (
              <PlatformIcon key={p} platform={p} size={18} />
            ))}
          </div>
          <p className="text-[11px] text-muted font-data mt-1">
            {creator.profiles.length} profile{creator.profiles.length === 1 ? "" : "s"}
          </p>
        </div>
        <button
          onClick={() => setModalOpen(true)}
          className="border border-accent text-accent px-4 py-2 text-sm hover:bg-accent hover:text-black transition-colors"
        >
          Edit creator
        </button>
      </div>

      {error && <p className="text-red-400 text-xs">{error}</p>}

      <div className="space-y-3">
        <h3 className="text-[10px] text-muted font-data uppercase tracking-widest">
          Posts
        </h3>
        {postsLoading && posts.length === 0 ? (
          <div className="flex items-center justify-center h-64 text-muted font-data text-xs tracking-widest animate-pulse">
            LOADING POSTS
          </div>
        ) : (
          <PostsTable
            rows={posts}
            pagination
            extraColumns={[
              {
                field: "relative_performance",
                headerName: "Relative Performance",
                width: 170,
                cellClass: "flex !items-center !justify-center",
                cellRenderer: ({ value }: { value: string | null }) => {
                  if (value === "hot") return <Badge variant="red">Hot</Badge>;
                  if (value === "standout") return <Badge variant="yellow">Standout</Badge>;
                  return <span className="text-muted">—</span>;
                },
              } satisfies ColDef<PostRow>,
            ]}
          />
        )}
      </div>

      <CreatorModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        onSaved={refreshAll}
        creator={creator}
      />
    </div>
  );
}
