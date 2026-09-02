import { cn } from "@/lib/utils";
import { useState } from "react";
import {
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
  Link,
  RouterProvider,
} from "@tanstack/react-router";
import { LayoutDashboard, Zap, FileText, Users, Activity, Menu, X } from "lucide-react";
import OverviewPage from "@/routes/overview";
import SignalsPage from "@/routes/signals";
import PostsPage from "@/routes/posts";
import CreatorsPage from "@/routes/creators";
import CreatorPage from "@/routes/creator";
import PostDetailPage from "@/routes/post-detail";

function RootLayout() {
  const [sidebarOpen, setSidebarOpen] = useState(false);

  return (
    <div className="min-h-screen bg-bg flex flex-col grain-overlay">
      {/* Header */}
      <header className="border-b border-border bg-surface/80 backdrop-blur-sm sticky top-0 z-50">
        <div className="max-w-[1440px] mx-auto px-4 sm:px-6 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <button
              onClick={() => setSidebarOpen(!sidebarOpen)}
              className="lg:hidden border border-border p-1.5 hover:border-accent transition-colors"
              aria-label="Toggle sidebar"
            >
              {sidebarOpen ? (
                <X className="w-4 h-4 text-muted" />
              ) : (
                <Menu className="w-4 h-4 text-muted" />
              )}
            </button>
            <Link to="/" className="flex items-center gap-2.5">
              <div className="w-7 h-7 sm:w-8 sm:h-8 border border-accent flex items-center justify-center">
                <Activity className="w-3.5 h-3.5 sm:w-4 sm:h-4 text-accent" />
              </div>
              <span className="text-sm font-bold tracking-[-0.03em] text-white hidden sm:inline">
                LAKEHOUSE
              </span>
            </Link>
          </div>
        </div>
      </header>

      <div className="flex flex-1 relative">
        {/* Mobile overlay */}
        {sidebarOpen && (
          <div
            className="fixed inset-0 bg-black/60 z-40 lg:hidden"
            onClick={() => setSidebarOpen(false)}
          />
        )}

        {/* Sidebar */}
        <aside
          className={cn(
            "fixed lg:sticky top-[49px] lg:top-[53px] z-50",
            "w-52 h-[calc(100vh-49px)] lg:h-[calc(100vh-53px)]",
            "border-r border-border bg-surface flex-shrink-0",
            "transition-transform duration-200",
            "lg:translate-x-0",
            sidebarOpen ? "translate-x-0" : "-translate-x-full",
          )}
        >
          <nav className="p-3 space-y-0.5">
            <NavLink to="/" icon={<LayoutDashboard className="w-4 h-4" />} onClick={() => setSidebarOpen(false)}>
              Overview
            </NavLink>
            <NavLink to="/signals" icon={<Zap className="w-4 h-4" />} onClick={() => setSidebarOpen(false)}>
              Signals
            </NavLink>
            <NavLink to="/posts" icon={<FileText className="w-4 h-4" />} onClick={() => setSidebarOpen(false)}>
              Posts
            </NavLink>
            <NavLink to="/creators" icon={<Users className="w-4 h-4" />} onClick={() => setSidebarOpen(false)}>
              Creators
            </NavLink>
          </nav>
          <div className="absolute bottom-0 left-0 right-0 p-4 border-t border-border">
            <div className="text-[10px] text-muted font-data uppercase tracking-widest">
              DATALAKE v1.0
            </div>
          </div>
        </aside>

        {/* Main */}
        <main className="flex-1 p-4 sm:p-6 overflow-auto w-full">
          <Outlet />
        </main>
      </div>
    </div>
  );
}

function NavLink({
  to,
  icon,
  children,
  onClick,
}: {
  to: string;
  icon: React.ReactNode;
  children: React.ReactNode;
  onClick?: () => void;
}) {
  return (
    <Link
      to={to}
      onClick={onClick}
      className={cn(
        "flex items-center gap-3 px-3 py-2.5 text-[13px] transition-colors border border-transparent",
        "text-muted hover:text-[#C9D4D4] hover:bg-[#1c1818] hover:border-border",
        "font-medium tracking-[-0.01em]",
      )}
      activeProps={{
        className:
          "text-accent border-accent bg-[#1c1818] hover:text-accent hover:border-accent hover:bg-[#1c1818]",
      }}
    >
      {icon}
      {children}
    </Link>
  );
}

// ── Router ─────────────────────────────────────────────────────

const rootRoute = createRootRoute({ component: RootLayout });
const indexRoute = createRoute({ getParentRoute: () => rootRoute, path: "/", component: OverviewPage });
const signalsRoute = createRoute({ getParentRoute: () => rootRoute, path: "/signals", component: SignalsPage });
const postsRoute = createRoute({ getParentRoute: () => rootRoute, path: "/posts", component: PostsPage });
const creatorsRoute = createRoute({ getParentRoute: () => rootRoute, path: "/creators", component: CreatorsPage });
const creatorRoute = createRoute({ getParentRoute: () => rootRoute, path: "/creators/$id", component: CreatorPage });
const postDetailRoute = createRoute({ getParentRoute: () => rootRoute, path: "/posts/$postId", component: PostDetailPage });
const routeTree = rootRoute.addChildren([indexRoute, signalsRoute, postsRoute, creatorsRoute, creatorRoute, postDetailRoute]);
const router = createRouter({ routeTree });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

export default function App() {
  return <RouterProvider router={router} />;
}
