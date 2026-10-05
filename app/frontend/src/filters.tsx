import { createContext, useContext, useMemo, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { X } from "lucide-react";
import { api, type Filters } from "./api";

// Global filters live in the URL (?region=…&start_date=…) so a view can be
// shared with a colleague and survives page reloads.
const KEYS = ["region", "super_region", "category", "channel", "start_date", "end_date"] as const;

type Ctx = { filters: Filters; set: (patch: Partial<Filters>) => void; clear: () => void };
const FilterCtx = createContext<Ctx | null>(null);

export function FilterProvider({ children }: { children: ReactNode }) {
  const [sp, setSp] = useSearchParams();
  const filters = useMemo(() => {
    const f: Filters = {};
    for (const k of KEYS) {
      const v = sp.get(k);
      if (v) f[k] = v;
    }
    return f;
  }, [sp]);
  const set = (patch: Partial<Filters>) =>
    setSp((prev) => {
      const next = new URLSearchParams(prev);
      for (const [k, v] of Object.entries(patch)) v ? next.set(k, v) : next.delete(k);
      return next;
    });
  const clear = () =>
    setSp((prev) => {
      const next = new URLSearchParams(prev);
      KEYS.forEach((k) => next.delete(k));
      return next;
    });
  return <FilterCtx.Provider value={{ filters, set, clear }}>{children}</FilterCtx.Provider>;
}

export function useFilters() {
  const ctx = useContext(FilterCtx);
  if (!ctx) throw new Error("useFilters outside FilterProvider");
  return ctx;
}

function monthBounds(month: string): { start_date: string; end_date: string } {
  const [y, m] = month.split("-").map(Number);
  const last = new Date(Date.UTC(y, m, 0)).getUTCDate();
  return { start_date: `${month}-01`, end_date: `${month}-${String(last).padStart(2, "0")}` };
}

function selectedMonth(f: Filters): string {
  // A date range that exactly spans one calendar month is shown as that month.
  if (!f.start_date || !f.end_date || !f.start_date.endsWith("-01")) return "";
  const month = f.start_date.slice(0, 7);
  return monthBounds(month).end_date === f.end_date ? month : "";
}

export function FilterBar({ show = KEYS as unknown as string[] }: { show?: string[] }) {
  const { filters: f, set, clear } = useFilters();
  const { data: opts } = useQuery({ queryKey: ["filters"], queryFn: api.filters, staleTime: Infinity });
  const regions = (opts?.regions ?? []).filter((r) => !f.super_region || r.super_region === f.super_region);
  const active = Object.keys(f).length > 0;
  const month = selectedMonth(f);
  const minMonth = opts?.min_date?.slice(0, 7);
  const maxMonth = opts?.max_date?.slice(0, 7);

  return (
    <div className="flex flex-wrap items-end gap-3 rounded-lg border border-oat-300 bg-white p-3 shadow-card">
      {show.includes("super_region") && (
        <Field label="Super region">
          <select className="input" value={f.super_region ?? ""}
            onChange={(e) => set({ super_region: e.target.value, region: undefined })}>
            <option value="">All</option>
            {opts?.super_regions.map((s) => <option key={s}>{s}</option>)}
          </select>
        </Field>
      )}
      {show.includes("region") && (
        <Field label="Region">
          <select className="input" value={f.region ?? ""} onChange={(e) => set({ region: e.target.value })}>
            <option value="">All</option>
            {regions.map((r) => <option key={r.region_name}>{r.region_name}</option>)}
          </select>
        </Field>
      )}
      {show.includes("category") && (
        <Field label="Product category">
          <select className="input" value={f.category ?? ""} onChange={(e) => set({ category: e.target.value })}>
            <option value="">All</option>
            {opts?.categories.map((c) => <option key={c}>{c}</option>)}
          </select>
        </Field>
      )}
      {show.includes("channel") && (
        <Field label="Channel">
          <select className="input" value={f.channel ?? ""} onChange={(e) => set({ channel: e.target.value })}>
            <option value="">All</option>
            {opts?.channels.map((c) => <option key={c}>{c}</option>)}
          </select>
        </Field>
      )}
      {show.includes("start_date") && (
        <>
          <Field label="Month">
            <input type="month" className="input" value={month} min={minMonth} max={maxMonth}
              onChange={(e) => (e.target.value ? set(monthBounds(e.target.value)) : set({ start_date: undefined, end_date: undefined }))} />
          </Field>
          <span className="pb-2 text-xs text-navy-300">or</span>
          <Field label="From">
            <input type="date" className="input" value={f.start_date ?? ""} min={opts?.min_date} max={opts?.max_date}
              onChange={(e) => set({ start_date: e.target.value })} />
          </Field>
          <Field label="To">
            <input type="date" className="input" value={f.end_date ?? ""} min={f.start_date ?? opts?.min_date} max={opts?.max_date}
              onChange={(e) => set({ end_date: e.target.value })} />
          </Field>
        </>
      )}
      {active && (
        <button className="btn-ghost" onClick={clear}>
          <X size={14} /> Clear
        </button>
      )}
      {opts && (
        <span className="ml-auto pb-2 text-xs text-navy-300">
          Data: {opts.min_date} → {opts.max_date}
        </span>
      )}
    </div>
  );
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-[11px] font-semibold uppercase tracking-wide text-navy-500">{label}</span>
      {children}
    </label>
  );
}
