import { NavLink, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import clsx from "clsx";
import { BarChart3, LayoutDashboard, Package, ShoppingCart, Sparkles, UserSearch } from "lucide-react";
import { api } from "./api";
import { FilterProvider } from "./filters";
import Overview from "./pages/Overview";
import Orders from "./pages/Orders";
import Customers from "./pages/Customers";
import Products from "./pages/Products";
import Sales from "./pages/Sales";
import Genie from "./pages/Genie";

const NAV = [
  { to: "/", label: "Overview", icon: LayoutDashboard },
  { to: "/customers", label: "Customer lookup", icon: UserSearch },
  { to: "/orders", label: "Orders", icon: ShoppingCart },
  { to: "/products", label: "Products", icon: Package },
  { to: "/sales", label: "Sales & revenue", icon: BarChart3 },
  { to: "/genie", label: "Ask Genie", icon: Sparkles },
];

// Global filters (region/category/dates) are carried across the filter-aware pages.
const FILTER_KEYS = ["region", "super_region", "category", "channel", "start_date", "end_date"];

export default function App() {
  const { search } = useLocation();
  const me = useQuery({ queryKey: ["whoami"], queryFn: api.whoami, staleTime: Infinity });
  const carried = new URLSearchParams([...new URLSearchParams(search)].filter(([k]) => FILTER_KEYS.includes(k))).toString();

  return (
    <div className="flex min-h-screen">
      <aside className="sticky top-0 flex h-screen w-60 shrink-0 flex-col bg-navy text-oat">
        <div className="px-5 py-5">
          <div className="text-[11px] font-semibold uppercase tracking-widest text-lava">NexusRetail</div>
          <div className="text-lg font-semibold">Support Console</div>
        </div>
        <nav className="flex flex-1 flex-col gap-0.5 px-3">
          {NAV.map(({ to, label, icon: Icon }) => (
            <NavLink key={to} to={["/", "/orders", "/sales"].includes(to) && carried ? `${to}?${carried}` : to} end={to === "/"}
              className={({ isActive }) => clsx("flex items-center gap-2.5 rounded-md px-3 py-2 text-sm transition",
                isActive ? "bg-white/10 font-medium text-white" : "text-oat/70 hover:bg-white/5 hover:text-white")}>
              <Icon size={16} /> {label}
            </NavLink>
          ))}
        </nav>
        <div className="border-t border-white/10 px-5 py-3 text-xs text-oat/60">
          <div className="truncate">{me.data?.email ?? "…"}</div>
          <div>gold + metrics schemas</div>
        </div>
      </aside>
      <main className="min-w-0 flex-1">
        <FilterProvider>
          <Routes>
            <Route path="/" element={<Overview />} />
            <Route path="/customers" element={<Customers />} />
            <Route path="/orders" element={<Orders />} />
            <Route path="/products" element={<Products />} />
            <Route path="/sales" element={<Sales />} />
            <Route path="/genie" element={<Genie />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </FilterProvider>
      </main>
    </div>
  );
}
