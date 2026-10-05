import clsx from "clsx";
import type { ReactNode } from "react";
import { AlertTriangle, Download, Inbox, Loader2 } from "lucide-react";
import { downloadCsv } from "@/format";
import type { Row } from "@/api";

// Categorical palette for charts — Lava first, then cool tones that stay distinct.
export const PALETTE = ["#FF3621", "#1B3139", "#00A972", "#2272B4", "#FFAB00", "#98102A", "#6A4C93", "#90A0A5"];

export function Page({ title, subtitle, actions, children }: {
  title: string; subtitle?: string; actions?: ReactNode; children: ReactNode;
}) {
  return (
    <div className="mx-auto flex max-w-[1400px] flex-col gap-4 p-6">
      <div className="flex items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">{title}</h1>
          {subtitle && <p className="text-sm text-navy-500">{subtitle}</p>}
        </div>
        {actions}
      </div>
      {children}
    </div>
  );
}

export function Card({ title, subtitle, actions, className, children }: {
  title?: string; subtitle?: string; actions?: ReactNode; className?: string; children: ReactNode;
}) {
  return (
    <section className={clsx("rounded-lg border border-oat-300 bg-white shadow-card", className)}>
      {(title || actions) && (
        <header className="flex items-start justify-between gap-3 border-b border-oat-200 px-4 py-3">
          <div>
            {title && <h2 className="text-sm font-semibold">{title}</h2>}
            {subtitle && <p className="text-xs text-navy-500">{subtitle}</p>}
          </div>
          {actions}
        </header>
      )}
      <div className="p-4">{children}</div>
    </section>
  );
}

export function Kpi({ label, value, hint, tone = "default" }: {
  label: string; value: ReactNode; hint?: string; tone?: "default" | "warn" | "bad";
}) {
  return (
    <div className="rounded-lg border border-oat-300 bg-white px-4 py-3 shadow-card">
      <div className="text-[11px] font-semibold uppercase tracking-wide text-navy-500">{label}</div>
      <div className={clsx("mt-1 text-2xl font-semibold tabular-nums",
        tone === "warn" && "text-amber-600", tone === "bad" && "text-lava-700")}>{value}</div>
      {hint && <div className="text-xs text-navy-300">{hint}</div>}
    </div>
  );
}

const STATUS_TONE: Record<string, string> = {
  delivered: "bg-emerald-50 text-emerald-700 ring-emerald-200",
  shipped: "bg-sky-50 text-sky-700 ring-sky-200",
  confirmed: "bg-indigo-50 text-indigo-700 ring-indigo-200",
  pending: "bg-amber-50 text-amber-700 ring-amber-200",
  cancelled: "bg-rose-50 text-rose-700 ring-rose-200",
  paid: "bg-emerald-50 text-emerald-700 ring-emerald-200",
  overdue: "bg-rose-50 text-rose-700 ring-rose-200",
  High: "bg-emerald-50 text-emerald-700 ring-emerald-200",
  Medium: "bg-sky-50 text-sky-700 ring-sky-200",
  Low: "bg-oat-200 text-navy-500 ring-oat-300",
  Churned: "bg-rose-50 text-rose-700 ring-rose-200",
};

export function Badge({ children, tone }: { children: ReactNode; tone?: string }) {
  const key = tone ?? String(children);
  return (
    <span className={clsx("inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ring-1 ring-inset",
      STATUS_TONE[key] ?? "bg-oat-200 text-navy-700 ring-oat-300")}>
      {children}
    </span>
  );
}

export function Loading({ label = "Loading…" }: { label?: string }) {
  return (
    <div className="flex items-center justify-center gap-2 py-10 text-sm text-navy-500">
      <Loader2 className="animate-spin" size={16} /> {label}
    </div>
  );
}

export function ErrorBox({ error }: { error: unknown }) {
  return (
    <div className="flex items-start gap-2 rounded-md border border-rose-200 bg-rose-50 p-3 text-sm text-rose-800">
      <AlertTriangle size={16} className="mt-0.5 shrink-0" />
      <div><div className="font-medium">Couldn't load data</div><div className="break-all">{String((error as Error)?.message ?? error)}</div></div>
    </div>
  );
}

export function Empty({ label = "No rows match the current filters." }: { label?: string }) {
  return (
    <div className="flex flex-col items-center gap-2 py-10 text-sm text-navy-500">
      <Inbox size={20} /> {label}
    </div>
  );
}

/** Renders loading / error / content for a react-query result. */
export function Async<T>({ q, children, loadingLabel }: {
  q: { isLoading: boolean; error: unknown; data: T | undefined };
  children: (data: T) => ReactNode;
  loadingLabel?: string;
}) {
  if (q.isLoading) return <Loading label={loadingLabel} />;
  if (q.error) return <ErrorBox error={q.error} />;
  if (q.data === undefined) return null;
  return <>{children(q.data)}</>;
}

export type Column = {
  key: string;
  label: string;
  align?: "right";
  render?: (row: Row) => ReactNode;
};

export function DataTable({ rows, columns, onRowClick, empty, maxHeight }: {
  rows: Row[]; columns: Column[]; onRowClick?: (row: Row) => void; empty?: string; maxHeight?: number;
}) {
  if (!rows.length) return <Empty label={empty} />;
  return (
    <div className="overflow-auto rounded-md border border-oat-200" style={maxHeight ? { maxHeight } : undefined}>
      <table className="min-w-full divide-y divide-oat-200">
        <thead className="sticky top-0 bg-oat">
          <tr>{columns.map((c) => <th key={c.key} className={clsx("th", c.align === "right" && "text-right")}>{c.label}</th>)}</tr>
        </thead>
        <tbody className="divide-y divide-oat-200 bg-white">
          {rows.map((r, i) => (
            <tr key={i} onClick={onRowClick ? () => onRowClick(r) : undefined}
              className={clsx(onRowClick && "cursor-pointer hover:bg-lava-50")}>
              {columns.map((c) => (
                <td key={c.key} className={clsx("td", c.align === "right" && "text-right tabular-nums")}>
                  {c.render ? c.render(r) : (r[c.key] ?? "—")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function CsvButton({ name, rows }: { name: string; rows: Row[] }) {
  return (
    <button className="btn-ghost h-7 px-2 text-xs" disabled={!rows.length} onClick={() => downloadCsv(name, rows)}>
      <Download size={13} /> CSV
    </button>
  );
}

export function Drawer({ open, onClose, title, children }: {
  open: boolean; onClose: () => void; title: string; children: ReactNode;
}) {
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-40 flex justify-end bg-navy/30" onClick={onClose}>
      <div className="h-full w-full max-w-5xl overflow-y-auto bg-oat shadow-2xl" onClick={(e) => e.stopPropagation()}>
        <div className="sticky top-0 z-10 flex items-center justify-between border-b border-oat-300 bg-white px-5 py-3">
          <h2 className="text-lg font-semibold">{title}</h2>
          <button className="btn-ghost" onClick={onClose}>Close</button>
        </div>
        <div className="flex flex-col gap-4 p-5">{children}</div>
      </div>
    </div>
  );
}

export function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div>
      <div className="text-[11px] font-semibold uppercase tracking-wide text-navy-500">{label}</div>
      <div className="text-sm">{children}</div>
    </div>
  );
}
