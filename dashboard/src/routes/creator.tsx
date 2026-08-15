import { useEffect, useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
import { Card } from "@/components/ui/card";
import { Avatar } from "@/components/ui/avatar";
import { Thumbnail } from "@/components/ui/thumbnail";
import { PlatformIcon } from "@/components/ui/platform-icon";
import { DataTable } from "@/components/ui/data-table";
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
        <DataTable
          columns={[
            {
              key: "thumbnail",
              header: "",
              sortable: false,
              render: (row: PostRow) => (
                <Thumbnail shortcode={row.shortcode} size={80} />
              ),
            },
            {
              key: "caption",
              header: "Caption",
              render: (row: PostRow) => (
                <span className="text-muted">
                  {row.caption || <span className="italic">no caption</span>}
                </span>
              ),
            },
            {
              key: "platform",
              header: "Platform",
              render: (row: PostRow) => <PlatformIcon platform={row.platform} size={16} />,
            },
            {
              key: "likes_count",
              header: "Likes",
              className: "text-center",
              render: (row: PostRow) => (
                <span className="text-muted">{row.likes_count.toLocaleString()}</span>
              ),
            },
            {
              key: "comments_count",
              header: "Comments",
              className: "text-center",
              render: (row: PostRow) => (
                <span className="text-muted">{row.comments_count.toLocaleString()}</span>
              ),
            },
            {
              key: "video_view_count",
              header: "Views",
              className: "text-center",
              render: (row: PostRow) => (
                <span className="text-muted">{row.video_view_count.toLocaleString()}</span>
              ),
            },
            {
              key: "relative_performance",
              header: "Relative Performance",
              className: "text-center",
              sortable: false,
              render: (row: PostRow) => {
                if (row.relative_performance === "hot") {
                  return <Badge variant="red">Hot</Badge>;
                }
                if (row.relative_performance === "standout") {
                  return <Badge variant="yellow">Standout</Badge>;
                }
                return <span className="text-muted">—</span>;
              },
            },
            {
              key: "timestamp",
              header: "Date",
              className: "text-right",
              render: (row: PostRow) => (
                <span className="text-muted">
                  {row.timestamp ? row.timestamp.slice(0, 10) : "--"}
                </span>
              ),
            },
          ]}
          data={posts}
          rowKey={(r) => r.post_id}
          loading={postsLoading}
          emptyMessage="No posts yet."
        />
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
