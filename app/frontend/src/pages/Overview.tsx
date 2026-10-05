import { useQuery } from "@tanstack/react-query";
import { Area, AreaChart, Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api } from "@/api";
import { useFilters, FilterBar } from "@/filters";
import { fmt } from "@/format";
import { Async, Card, Kpi, PALETTE, Page } from "@/components/ui";

const STATUS_COLOR: Record<string, string> = {
  delivered: "#00A972", shipped: "#2272B4", confirmed: "#6A4C93", pending: "#FFAB00", cancelled: "#FF3621",
};

export default function Overview() {
  const { filters } = useFilters();
  const q = useQuery({ queryKey: ["overview", filters], queryFn: () => api.overview(filters) });
  return (
    <Page title="Overview" subtitle="Order health for the selected region, category and period.">
      <FilterBar />
      <Async q={q}>
        {({ kpis: k, trend, by_status }) => (
          <>
            <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-5">
              <Kpi label="Orders" value={fmt.num(k.orders)} hint={`${fmt.num(k.customers)} customers`} />
              <Kpi label="Revenue" value={fmt.money(k.revenue)} hint="excl. cancelled orders" />
              <Kpi label="Avg order value" value={fmt.money2(k.avg_order_value)} />
              <Kpi label="Open orders" value={fmt.num(k.open_orders)} hint="pending + confirmed" tone={k.open_orders ? "warn" : "default"} />
              <Kpi label="Overdue invoices" value={fmt.num(k.overdue_invoices)} tone={k.overdue_invoices ? "bad" : "default"} />
              <Kpi label="Return rate" value={fmt.pct(k.return_rate_pct)} />
              <Kpi label="Cancellation rate" value={fmt.pct(k.cancellation_rate_pct)} />
              <Kpi label="Refunds issued" value={fmt.money(k.refunds)} />
            </div>
            <div className="grid gap-4 lg:grid-cols-3">
              <Card title="Monthly revenue" subtitle="Excludes cancelled orders" className="lg:col-span-2">
                <ResponsiveContainer width="100%" height={280}>
                  <AreaChart data={trend} margin={{ left: 8, right: 8 }}>
                    <defs>
                      <linearGradient id="rev" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="0%" stopColor={PALETTE[0]} stopOpacity={0.35} />
                        <stop offset="100%" stopColor={PALETTE[0]} stopOpacity={0} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid stroke="#EEEDE9" vertical={false} />
                    <XAxis dataKey="month" tickFormatter={fmt.month} fontSize={11} />
                    <YAxis tickFormatter={fmt.moneyCompact} fontSize={11} width={56} />
                    <Tooltip labelFormatter={fmt.month} formatter={(v: number, n) => (n === "revenue" ? fmt.money(v) : fmt.num(v))} />
                    <Area isAnimationActive={false} type="monotone" dataKey="revenue" stroke={PALETTE[0]} fill="url(#rev)" strokeWidth={2} />
                  </AreaChart>
                </ResponsiveContainer>
              </Card>
              <Card title="Orders by status">
                <ResponsiveContainer width="100%" height={280}>
                  <BarChart data={by_status} layout="vertical" margin={{ left: 16 }}>
                    <CartesianGrid stroke="#EEEDE9" horizontal={false} />
                    <XAxis type="number" fontSize={11} />
                    <YAxis type="category" dataKey="order_status" fontSize={12} width={72} />
                    <Tooltip formatter={(v: number) => fmt.num(v)} />
                    <Bar isAnimationActive={false} dataKey="orders" radius={[0, 4, 4, 0]}>
                      {by_status.map((s) => <Cell key={s.order_status} fill={STATUS_COLOR[s.order_status] ?? PALETTE[7]} />)}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </Card>
            </div>
          </>
        )}
      </Async>
    </Page>
  );
}
