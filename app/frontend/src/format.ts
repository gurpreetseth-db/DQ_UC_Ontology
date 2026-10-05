const money = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
const money2 = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 2 });
const num = new Intl.NumberFormat("en-US");
const compact = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });

export const fmt = {
  money: (v: unknown) => (v == null ? "—" : money.format(Number(v))),
  money2: (v: unknown) => (v == null ? "—" : money2.format(Number(v))),
  moneyCompact: (v: unknown) => (v == null ? "—" : "$" + compact.format(Number(v))),
  num: (v: unknown) => (v == null ? "—" : num.format(Number(v))),
  pct: (v: unknown) => (v == null ? "—" : `${Number(v).toFixed(1)}%`),
  date: (v: unknown) => (v ? String(v).slice(0, 10) : "—"),
  month: (v: unknown) => {
    if (!v) return "—";
    const d = new Date(String(v).slice(0, 10) + "T00:00:00Z");
    return d.toLocaleDateString("en-US", { month: "short", year: "2-digit", timeZone: "UTC" });
  },
};

export function toCsv(rows: Record<string, unknown>[]): string {
  if (!rows.length) return "";
  const cols = Object.keys(rows[0]);
  const esc = (v: unknown) => {
    const s = v == null ? "" : String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [cols.join(","), ...rows.map((r) => cols.map((c) => esc(r[c])).join(","))].join("\n");
}

export function downloadCsv(name: string, rows: Record<string, unknown>[]) {
  const blob = new Blob([toCsv(rows)], { type: "text/csv" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = name;
  a.click();
  URL.revokeObjectURL(a.href);
}
