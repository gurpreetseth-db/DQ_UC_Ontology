import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api, type Row } from "@/api";
import { FilterBar, useFilters } from "@/filters";
import { fmt } from "@/format";
import { Async, Card, CsvButton, DataTable, Kpi, PALETTE, Page } from "@/components/ui";

export default function Sales() {
  const { filters } = useFilters();
  const q = useQuery({ queryKey: ["sales", filters], queryFn: () => api.sales(filters) });
  return (
    <Page title="Sales & revenue" subtitle="Revenue by product category, region and month — from the certified metrics layer.">
      <FilterBar />
      <Async q={q}>{(d) => <SalesBody d={d} />}</Async>
    </Page>
  );
}

function SalesBody({ d }: { d: { by_category: Row[]; by_region: Row[]; matrix: Row[]; trend: Row[]; notes: Record<string, string> } }) {
  const [metric, setMetric] = useState<"gross_revenue" | "net_revenue">("gross_revenue");
  const totals = useMemo(() => d.by_category.reduce(
    (a, r) => ({ gross: a.gross + r.gross_revenue, net: a.net + r.net_revenue, disc: a.disc + r.discounts, ref: a.ref + r.refunds, units: a.units + r.units }),
    { gross: 0, net: 0, disc: 0, ref: 0, units: 0 }), [d.by_category]);

  // Pivot the trend (month × channel rows) into one row per month for a multi-line chart.
  const { trendRows, channels } = useMemo(() => {
    const byMonth = new Map<string, Row>();
    const ch = new Set<string>();
    for (const r of d.trend) {
      ch.add(r.channel);
      const row = byMonth.get(r.month) ?? { month: r.month };
      row[r.channel] = r.gross_revenue;
      byMonth.set(r.month, row);
    }
    return { trendRows: [...byMonth.values()], channels: [...ch].sort() };
  }, [d.trend]);

  const { regions, matrixRows } = useMemo(() => {
    const regs = [...new Set(d.matrix.map((m) => m.region_name))].sort();
    const byCat = new Map<string, Row>();
    for (const m of d.matrix) {
      const row = byCat.get(m.category_name) ?? { category_name: m.category_name, total: 0 };
      row[m.region_name] = m.gross_revenue;
      row.total += m.gross_revenue;
      byCat.set(m.category_name, row);
    }
    return { regions: regs, matrixRows: [...byCat.values()].sort((a, b) => b.total - a.total) };
  }, [d.matrix]);
  const maxCell = Math.max(1, ...d.matrix.map((m) => m.gross_revenue));

  return (
    <>
      <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
        <Kpi label="Gross revenue" value={fmt.money(totals.gross)} />
        <Kpi label="Net revenue" value={fmt.money(totals.net)} hint="after discounts" />
        <Kpi label="Discounts" value={fmt.money(totals.disc)} />
        <Kpi label="Refunds" value={fmt.money(totals.ref)} />
        <Kpi label="Units sold" value={fmt.num(totals.units)} />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card title="Revenue by product category" subtitle={d.notes.by_category}
          actions={<select className="input h-7 text-xs" value={metric} onChange={(e) => setMetric(e.target.value as typeof metric)}>
            <option value="gross_revenue">Gross</option><option value="net_revenue">Net</option></select>}>
          <ResponsiveContainer width="100%" height={340}>
            <BarChart data={d.by_category} layout="vertical" margin={{ left: 24 }}>
              <CartesianGrid stroke="#EEEDE9" horizontal={false} />
              <XAxis type="number" tickFormatter={fmt.moneyCompact} fontSize={11} />
              <YAxis type="category" dataKey="category_name" width={140} fontSize={11} />
              <Tooltip formatter={(v: number) => fmt.money(v)} />
              <Bar isAnimationActive={false} dataKey={metric} fill={PALETTE[0]} radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </Card>
        <Card title="Revenue by region" subtitle={d.notes.by_region}>
          <ResponsiveContainer width="100%" height={340}>
            <BarChart data={d.by_region}>
              <CartesianGrid stroke="#EEEDE9" vertical={false} />
              <XAxis dataKey="region_name" fontSize={11} />
              <YAxis tickFormatter={fmt.moneyCompact} fontSize={11} width={56} />
              <Tooltip formatter={(v: number) => fmt.money(v)} />
              <Legend />
              <Bar isAnimationActive={false} dataKey="gross_revenue" name="Gross" fill={PALETTE[1]} radius={[3, 3, 0, 0]} />
              <Bar isAnimationActive={false} dataKey="net_revenue" name="Net" fill={PALETTE[3]} radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </Card>
      </div>

      <Card title="Monthly gross revenue by channel" subtitle={d.notes.trend}>
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={trendRows}>
            <CartesianGrid stroke="#EEEDE9" vertical={false} />
            <XAxis dataKey="month" tickFormatter={fmt.month} fontSize={11} />
            <YAxis tickFormatter={fmt.moneyCompact} fontSize={11} width={56} />
            <Tooltip labelFormatter={fmt.month} formatter={(v: number) => fmt.money(v)} />
            <Legend />
            {channels.map((c, i) => <Line isAnimationActive={false} key={c} dataKey={c} stroke={PALETTE[i]} strokeWidth={2} dot={false} />)}
          </LineChart>
        </ResponsiveContainer>
      </Card>

      <Card title="Category × region gross revenue" subtitle="Darker cells = higher revenue" actions={<CsvButton name="category_region_revenue.csv" rows={matrixRows} />}>
        <DataTable rows={matrixRows} columns={[
          { key: "category_name", label: "Category" },
          ...regions.map((reg) => ({
            key: reg, label: reg, align: "right" as const,
            render: (r: Row) => r[reg] == null ? "—" : (
              <span className="rounded px-1.5 py-0.5" style={{ background: `rgba(255,54,33,${(0.08 + 0.6 * r[reg] / maxCell).toFixed(2)})` }}>
                {fmt.moneyCompact(r[reg])}
              </span>),
          })),
          { key: "total", label: "Total", align: "right", render: (r) => <span className="font-semibold">{fmt.money(r.total)}</span> },
        ]} />
      </Card>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card title="Category detail" actions={<CsvButton name="revenue_by_category.csv" rows={d.by_category} />}>
          <DataTable rows={d.by_category} columns={[
            { key: "category_name", label: "Category" },
            { key: "orders", label: "Orders", align: "right", render: (r) => fmt.num(r.orders) },
            { key: "gross_revenue", label: "Gross", align: "right", render: (r) => fmt.money(r.gross_revenue) },
            { key: "net_revenue", label: "Net", align: "right", render: (r) => fmt.money(r.net_revenue) },
            { key: "refunds", label: "Refunds", align: "right", render: (r) => fmt.money(r.refunds) },
            { key: "returns", label: "Returns", align: "right" },
          ]} />
        </Card>
        <Card title="Region detail" actions={<CsvButton name="revenue_by_region.csv" rows={d.by_region} />}>
          <DataTable rows={d.by_region} columns={[
            { key: "region_name", label: "Region" },
            { key: "super_region", label: "Super region" },
            { key: "orders", label: "Orders", align: "right", render: (r) => fmt.num(r.orders) },
            { key: "gross_revenue", label: "Gross", align: "right", render: (r) => fmt.money(r.gross_revenue) },
            { key: "avg_order_value", label: "AOV", align: "right", render: (r) => fmt.money2(r.avg_order_value) },
            { key: "cancelled_orders", label: "Cancelled", align: "right" },
          ]} />
        </Card>
      </div>
    </>
  );
}
