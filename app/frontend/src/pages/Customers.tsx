import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { Search, UserRound } from "lucide-react";
import { api } from "@/api";
import { fmt } from "@/format";
import { Async, Badge, Card, DataTable, Drawer, Empty, Field, Kpi, Page } from "@/components/ui";
import { OrderDetail } from "@/components/OrderDetail";
import { ORDER_COLUMNS } from "./Orders";

export default function Customers() {
  const [sp, setSp] = useSearchParams();
  const selected = sp.get("id");
  const openOrder = sp.get("order");
  const [text, setText] = useState("");
  const [search, setSearch] = useState("");
  useEffect(() => { const t = setTimeout(() => setSearch(text.trim()), 300); return () => clearTimeout(t); }, [text]);

  const results = useQuery({ queryKey: ["customers", search], queryFn: () => api.customers(search), enabled: search.length > 0 });
  const detail = useQuery({ queryKey: ["customer", selected], queryFn: () => api.customer(selected!), enabled: !!selected });
  const setParam = (k: string, v: string | null) => setSp((prev) => {
    const n = new URLSearchParams(prev); v ? n.set(k, v) : n.delete(k); return n;
  });

  return (
    <Page title="Customer lookup" subtitle="Search by customer ID to see the customer's value profile and full order history.">
      <div className="grid gap-4 lg:grid-cols-[320px_1fr]">
        <Card title="Find customer">
          <div className="relative mb-3">
            <Search size={14} className="absolute left-2.5 top-2.5 text-navy-300" />
            <input autoFocus className="input w-full pl-8" placeholder="e.g. CUST-000123 or 123" value={text}
              onChange={(e) => setText(e.target.value)} />
          </div>
          {!search ? <p className="text-sm text-navy-500">Type part of a customer ID.</p> : (
            <Async q={results}>
              {(rows) => rows.length ? (
                <ul className="flex max-h-[60vh] flex-col divide-y divide-oat-200 overflow-auto">
                  {rows.map((r) => (
                    <li key={r.customer_id}>
                      <button onClick={() => setParam("id", r.customer_id)}
                        className={`flex w-full items-center justify-between px-2 py-2 text-left text-sm hover:bg-lava-50 ${selected === r.customer_id ? "bg-lava-50" : ""}`}>
                        <span>
                          <span className="font-mono">{r.customer_id}</span>
                          <span className="block text-xs text-navy-500">{r.region_name} · {r.orders} orders · last {fmt.date(r.last_order_date)}</span>
                        </span>
                        {r.clv_segment && <Badge>{r.clv_segment}</Badge>}
                      </button>
                    </li>
                  ))}
                </ul>
              ) : <Empty label="No customers match." />}
            </Async>
          )}
        </Card>

        <div className="flex flex-col gap-4">
          {!selected ? (
            <Card><div className="flex flex-col items-center gap-2 py-16 text-navy-500"><UserRound size={28} />Select a customer to see their orders.</div></Card>
          ) : (
            <Async q={detail}>
              {({ profile: p, orders, top_categories }) => (
                <>
                  <Card title={selected} subtitle="Customers are identified by ID only — no PII is exposed in this app."
                    actions={p && <Badge>{p.clv_segment}</Badge>}>
                    {p ? (
                      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
                        <Field label="Loyalty tier">{p.loyalty_tier}</Field>
                        <Field label="Customer type">{p.customer_type}</Field>
                        <Field label="Region / country">{orders[0]?.region_name ?? p.region_id} · {p.country_code}</Field>
                        <Field label="Age · income">{p.age_bracket} · {p.income_bracket}</Field>
                        <Field label="First order">{fmt.date(p.first_order_date)}</Field>
                        <Field label="Last order">{fmt.date(p.last_order_date)} ({p.days_since_last_order}d ago)</Field>
                        <Field label="Orders / 30 days">{p.orders_per_30_days ?? "—"}</Field>
                        <Field label="Return rate">{fmt.pct(p.customer_return_rate_pct)}</Field>
                      </div>
                    ) : <p className="text-sm text-navy-500">No lifetime-value profile yet — this customer has only pending or cancelled orders.</p>}
                  </Card>
                  <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
                    <Kpi label="Orders" value={fmt.num(orders.length)} />
                    <Kpi label="Lifetime revenue" value={fmt.money(p?.total_revenue)} hint="delivered/shipped/confirmed" />
                    <Kpi label="Avg order value" value={fmt.money2(p?.avg_order_value)} />
                    <Kpi label="Open orders" value={fmt.num(orders.filter((o) => ["pending", "confirmed", "shipped"].includes(o.order_status)).length)} />
                  </div>
                  <Card title="Order history" subtitle="Click an order to see line items, invoice and return.">
                    <DataTable rows={orders} columns={ORDER_COLUMNS.filter((c) => c.key !== "customer_id")}
                      onRowClick={(r) => setParam("order", r.order_id)} maxHeight={420} />
                  </Card>
                  {top_categories.length > 0 && (
                    <Card title="Top categories purchased">
                      <div className="flex flex-wrap gap-2">
                        {top_categories.map((c) => (
                          <span key={c.category_name} className="rounded-md bg-oat-200 px-2.5 py-1 text-sm">
                            {c.category_name} · <span className="font-medium">{fmt.money(c.spend)}</span> · {c.units} units
                          </span>
                        ))}
                      </div>
                    </Card>
                  )}
                </>
              )}
            </Async>
          )}
        </div>
      </div>
      <Drawer open={!!openOrder} onClose={() => setParam("order", null)} title={`Order ${openOrder ?? ""}`}>
        {openOrder && <OrderDetail orderId={openOrder} />}
      </Drawer>
    </Page>
  );
}
